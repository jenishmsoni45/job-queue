// Package bench turns a Black Friday run into numbers, checks them against the
// targets, and writes the report and the chart (design §17, D-027, D-029).
//
// Analyze reads one run's jobs, as exported from Postgres, and mockshop's
// mutation log, and computes the run's summary: per-shop queue waits (D-011),
// p99 queue wait in 10 s buckets by arrival (D-051), the warm-up backlog, and the
// checks for NFR-9, NFR-11 and NFR-12. Every time comes from the database clock.
// Report compares the FIFO and DRR runs; Chart draws them.
package bench

import (
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"
	"time"

	"github.com/jenishmsoni45/job-queue/internal/config"
	"github.com/jenishmsoni45/job-queue/internal/loadgen"
)

// BucketSize is the width of a chart and NFR-9 bucket (design §17).
const BucketSize = 10 * time.Second

// nfr9SlackMS is NFR-9's absolute slack: a bucket's p99 may be up to twice the
// shop's warm-up p99 plus 100 ms (D-027).
const nfr9SlackMS = 100

// nfr12Seconds is NFR-12's allowance: a shop's backlog stays within 2 s of its
// arrivals during the warm-up (D-029).
const nfr12Seconds = 2

// Job is one exported job.
type Job struct {
	ID              int64
	Shop            string
	State           string
	EnqueuedAt      time.Time
	FirstRunnableAt time.Time
	FirstClaimedAt  *time.Time
	// TerminalAt is completed_at or dead_at.
	TerminalAt *time.Time
	Attempts   int32
	Throttles  int32
	ClaimSeq   int32
}

// Dist summarizes a distribution of durations, in milliseconds.
type Dist struct {
	Count int     `json:"count"`
	P50   float64 `json:"p50_ms"`
	P99   float64 `json:"p99_ms"`
	Max   float64 `json:"max_ms"`
}

// Bucket is one shop's queue waits for the jobs that arrived in one 10 s bucket.
type Bucket struct {
	Index  int     `json:"index"`
	StartS float64 `json:"start_s"` // from the run's first arrival
	Count  int     `json:"count"`
	P50MS  float64 `json:"p50_ms"`
	P99MS  float64 `json:"p99_ms"`
}

// ShopSummary is one shop's numbers in one run.
type ShopSummary struct {
	ID        string `json:"id"`
	Jobs      int    `json:"jobs"` // accepted jobs: every job in the database
	Completed int    `json:"completed"`
	Dead      int    `json:"dead"`
	Shed      int64  `json:"shed"` // shop_state.shed_total: jobs refused with 429
	// QueueWait is first_claimed_at − first_runnable_at over every job (D-011),
	// and EndToEnd is completed_at − enqueued_at over completed jobs.
	QueueWait   Dist    `json:"queue_wait"`
	WarmupWait  Dist    `json:"warmup_queue_wait"`
	SpikeWait   Dist    `json:"spike_queue_wait"`
	EndToEnd    Dist    `json:"end_to_end"`
	WarmupMaxBL int     `json:"warmup_max_backlog"`
	Throughput  float64 `json:"completed_per_s"`
}

// Totals are the whole run's counts.
type Totals struct {
	Jobs        int   `json:"jobs"`
	Completed   int   `json:"completed"`
	Dead        int   `json:"dead"`
	NotTerminal int   `json:"not_terminal"`
	Shed        int64 `json:"shed"`
	// Claims is every claim of every job (the sum of claim_seq): runs, not
	// jobs. ExtraClaims is how many of them were reruns, which delivery at
	// least once allows. MutationsApplied is what mockshop applied, which
	// NFR-11 holds at one per job.
	Claims           int64   `json:"claims"`
	ExtraClaims      int64   `json:"extra_claims"`
	Attempts         int64   `json:"attempts"`
	Throttles        int64   `json:"throttles"`
	MutationsApplied int     `json:"mutations_applied"`
	Replays          int     `json:"mutation_replays"`
	DurationS        float64 `json:"duration_s"` // first arrival to last terminal job
	Throughput       float64 `json:"completed_per_s"`
	QueueWait        Dist    `json:"queue_wait"`
	EndToEnd         Dist    `json:"end_to_end"`
}

// NFR9Shop is NFR-9's check for one shop.
type NFR9Shop struct {
	ID          string  `json:"id"`
	WarmupP99MS float64 `json:"warmup_p99_ms"`
	BoundMS     float64 `json:"bound_ms"` // 2 × warm-up p99 + 100 ms
	WorstBucket int     `json:"worst_bucket"`
	WorstP99MS  float64 `json:"worst_p99_ms"`
	Buckets     int     `json:"buckets_checked"`
	Over        int     `json:"buckets_over"`
	Empty       int     `json:"buckets_empty"`
	Pass        bool    `json:"pass"`
}

// NFR9 is the fairness check: every shop but the spike shop keeps its p99
// queue wait in every 10 s bucket of the spike within 2 × its warm-up p99 +
// 100 ms (D-027). The target applies to the DRR run; the FIFO run's result is
// reported, not targeted.
type NFR9 struct {
	Pass  bool       `json:"pass"`
	Shops []NFR9Shop `json:"shops"`
}

// NFR11 is the one-effect-per-job check: every completed job has exactly one
// applied mutation, and no job has more than one (D-029).
type NFR11 struct {
	Pass             bool     `json:"pass"`
	Completed        int      `json:"completed"`
	CompletedNotOne  int      `json:"completed_without_exactly_one"`
	KeysOverOne      int      `json:"keys_applied_more_than_once"`
	Examples         []string `json:"examples,omitempty"`
	UnknownMutations int      `json:"mutations_for_no_job"`
}

// NFR12Shop is NFR-12's check for one shop.
type NFR12Shop struct {
	ID         string `json:"id"`
	MaxBacklog int    `json:"max_backlog"`
	Pass       bool   `json:"pass"`
}

// NFR12 is the baseline check: during the warm-up no shop's backlog passes 2 s
// of its arrivals (D-029). The backlog is the shop's jobs enqueued and not yet
// terminal, swept exactly over every enqueue and every terminal transition.
type NFR12 struct {
	Pass  bool        `json:"pass"`
	Limit int         `json:"limit_jobs"`
	Shops []NFR12Shop `json:"shops"`
}

// Run is one run's summary. It is written as run.json and committed, so the
// chart and the report can be rebuilt from it (D-052).
type Run struct {
	Scheduler    string    `json:"scheduler"`
	ConfigSHA256 string    `json:"config_sha256"`
	T0           time.Time `json:"t0"` // the run's first arrival, by the database clock
	WarmupS      float64   `json:"warmup_s"`
	SpikeS       float64   `json:"spike_s"`
	BucketS      float64   `json:"bucket_s"`
	SpikeShop    string    `json:"spike_shop"`
	// SpikeRate and SteadyRate are the configured arrival rates, in jobs per
	// second: settings, not measurements.
	SpikeRate  float64             `json:"spike_rate"`
	SteadyRate float64             `json:"steady_rate"`
	Totals     Totals              `json:"totals"`
	Shops      []ShopSummary       `json:"shops"`
	Series     map[string][]Bucket `json:"series"`
	NFR9       NFR9                `json:"nfr9"`
	NFR11      NFR11               `json:"nfr11"`
	NFR12      NFR12               `json:"nfr12"`
}

// Input is what Analyze needs about one run.
type Input struct {
	Scheduler    string
	ConfigSHA256 string
	Config       *config.File // the bench config, with its loadgen section
	Jobs         []Job
	// Mutations holds each shop's applied count per Idempotency-Key, and
	// Replays each shop's replayed calls, from mockshop's log.
	Mutations map[string]map[string]int
	Replays   map[string]int
	Shed      map[string]int64 // shop_state.shed_total
	// MutationKey returns the Idempotency-Key a job's tag call sends.
	MutationKey func(jobID int64) string
}

// Analyze computes one run's summary.
func Analyze(in Input) (Run, error) {
	lg := in.Config.Loadgen
	if lg == nil {
		return Run{}, errors.New("bench: the config has no loadgen section")
	}
	warmup := time.Duration(lg.WarmupMS) * time.Millisecond
	spike := time.Duration(lg.SpikeMS) * time.Millisecond
	if warmup%BucketSize != 0 || spike%BucketSize != 0 {
		return Run{}, fmt.Errorf("bench: the warm-up (%v) and the spike (%v) must be whole buckets of %v", warmup, spike, BucketSize)
	}
	if len(in.Jobs) == 0 {
		return Run{}, errors.New("bench: the run has no jobs")
	}
	r := Run{
		Scheduler: in.Scheduler, ConfigSHA256: in.ConfigSHA256, WarmupS: warmup.Seconds(), SpikeS: spike.Seconds(),
		BucketS: BucketSize.Seconds(), SpikeShop: lg.SpikeShop, SpikeRate: lg.SpikeRate, SteadyRate: lg.RatePerShop,
		Series: map[string][]Bucket{},
	}
	r.T0 = in.Jobs[0].FirstRunnableAt
	for _, j := range in.Jobs {
		if j.FirstRunnableAt.Before(r.T0) {
			r.T0 = j.FirstRunnableAt
		}
	}
	warmupEnd, spikeEnd := r.T0.Add(warmup), r.T0.Add(warmup+spike)
	firstSpikeBucket, spikeBuckets := int(warmup/BucketSize), int(spike/BucketSize)

	byShop := map[string][]Job{}
	var last time.Time
	var allWaits, allE2E []float64
	for _, j := range in.Jobs {
		byShop[j.Shop] = append(byShop[j.Shop], j)
		r.Totals.Jobs++
		r.Totals.Claims += int64(j.ClaimSeq)
		if j.ClaimSeq > 1 {
			r.Totals.ExtraClaims += int64(j.ClaimSeq - 1)
		}
		r.Totals.Attempts += int64(j.Attempts)
		r.Totals.Throttles += int64(j.Throttles)
		switch j.State {
		case "completed":
			r.Totals.Completed++
		case "dead":
			r.Totals.Dead++
		default:
			r.Totals.NotTerminal++
		}
		if j.TerminalAt != nil && j.TerminalAt.After(last) {
			last = *j.TerminalAt
		}
		if w, ok := wait(j); ok {
			allWaits = append(allWaits, w)
		}
		if e, ok := endToEnd(j); ok {
			allE2E = append(allE2E, e)
		}
	}
	r.Totals.DurationS = last.Sub(r.T0).Seconds()
	if r.Totals.DurationS > 0 {
		r.Totals.Throughput = float64(r.Totals.Completed) / r.Totals.DurationS
	}
	r.Totals.QueueWait, r.Totals.EndToEnd = dist(allWaits), dist(allE2E)

	r.NFR9.Pass = true
	r.NFR12 = NFR12{Pass: true, Limit: int(math.Floor(nfr12Seconds * lg.RatePerShop))}
	for _, shop := range in.Config.Shops {
		jobs := byShop[shop.ID]
		s := ShopSummary{ID: shop.ID, Jobs: len(jobs), Shed: in.Shed[shop.ID]}
		r.Totals.Shed += s.Shed
		var waits, warm, spk, e2e []float64
		perBucket := map[int][]float64{}
		for _, j := range jobs {
			switch j.State {
			case "completed":
				s.Completed++
			case "dead":
				s.Dead++
			}
			if e, ok := endToEnd(j); ok {
				e2e = append(e2e, e)
			}
			w, ok := wait(j)
			if !ok {
				continue
			}
			waits = append(waits, w)
			b := int(j.FirstRunnableAt.Sub(r.T0) / BucketSize)
			perBucket[b] = append(perBucket[b], w)
			switch {
			case j.FirstRunnableAt.Before(warmupEnd):
				warm = append(warm, w)
			case j.FirstRunnableAt.Before(spikeEnd):
				spk = append(spk, w)
			}
		}
		s.QueueWait, s.WarmupWait, s.SpikeWait, s.EndToEnd = dist(waits), dist(warm), dist(spk), dist(e2e)
		if r.Totals.DurationS > 0 {
			s.Throughput = float64(s.Completed) / r.Totals.DurationS
		}
		r.Series[shop.ID] = series(perBucket)

		// NFR-12: the warm-up backlog, swept exactly.
		s.WarmupMaxBL = maxBacklog(jobs, warmupEnd)
		ok12 := s.WarmupMaxBL <= r.NFR12.Limit
		r.NFR12.Pass = r.NFR12.Pass && ok12
		r.NFR12.Shops = append(r.NFR12.Shops, NFR12Shop{ID: shop.ID, MaxBacklog: s.WarmupMaxBL, Pass: ok12})

		// NFR-9, for every shop but the spike shop.
		if shop.ID != lg.SpikeShop {
			n := NFR9Shop{ID: shop.ID, WarmupP99MS: s.WarmupWait.P99, WorstBucket: -1, Pass: true}
			n.BoundMS = 2*n.WarmupP99MS + nfr9SlackMS
			if s.WarmupWait.Count == 0 {
				n.Pass = false // no warm-up, no bound: the check can't pass vacuously
			}
			for b := firstSpikeBucket; b < firstSpikeBucket+spikeBuckets; b++ {
				w := perBucket[b]
				if len(w) == 0 {
					n.Empty++
					continue
				}
				n.Buckets++
				p99 := quantile(w, 0.99)
				if n.WorstBucket < 0 || p99 > n.WorstP99MS {
					n.WorstBucket, n.WorstP99MS = b, p99
				}
				if p99 > n.BoundMS {
					n.Over++
					n.Pass = false
				}
			}
			if n.Buckets == 0 {
				n.Pass = false
			}
			r.NFR9.Pass = r.NFR9.Pass && n.Pass
			r.NFR9.Shops = append(r.NFR9.Shops, n)
		}
		r.Shops = append(r.Shops, s)
	}
	r.NFR11 = checkNFR11(in, byShop, &r.Totals)
	return r, nil
}

// checkNFR11 compares the completed jobs with mockshop's mutation log.
func checkNFR11(in Input, byShop map[string][]Job, t *Totals) NFR11 {
	n := NFR11{}
	example := func(format string, args ...any) {
		if len(n.Examples) < 10 {
			n.Examples = append(n.Examples, fmt.Sprintf(format, args...))
		}
	}
	for _, shop := range in.Config.Shops {
		applied := in.Mutations[shop.ID]
		t.Replays += in.Replays[shop.ID]
		known := map[string]bool{}
		for _, j := range byShop[shop.ID] {
			key := in.MutationKey(j.ID)
			known[key] = true
			if j.State != "completed" {
				continue
			}
			n.Completed++
			if c := applied[key]; c != 1 {
				n.CompletedNotOne++
				example("%s: completed job %d has %d applied mutations", shop.ID, j.ID, c)
			}
		}
		for _, key := range slices.Sorted(maps.Keys(applied)) {
			c := applied[key]
			t.MutationsApplied += c
			if c > 1 {
				n.KeysOverOne++
				example("%s: %s applied %d times", shop.ID, key, c)
			}
			if !known[key] {
				n.UnknownMutations++
				example("%s: %s matches no job of the run", shop.ID, key)
			}
		}
	}
	n.Pass = n.Completed > 0 && n.CompletedNotOne == 0 && n.KeysOverOne == 0 && n.UnknownMutations == 0
	return n
}

// wait is the job's queue wait in ms: first claim minus first runnable (D-011).
func wait(j Job) (float64, bool) {
	if j.FirstClaimedAt == nil {
		return 0, false
	}
	return ms(j.FirstClaimedAt.Sub(j.FirstRunnableAt)), true
}

// endToEnd is a completed job's time from enqueue to completion, in ms.
func endToEnd(j Job) (float64, bool) {
	if j.State != "completed" || j.TerminalAt == nil {
		return 0, false
	}
	return ms(j.TerminalAt.Sub(j.EnqueuedAt)), true
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

// series turns per-bucket waits into buckets, in order. Buckets with no
// arrivals are left out; the chart breaks its line there.
func series(perBucket map[int][]float64) []Bucket {
	idx := make([]int, 0, len(perBucket))
	for b := range perBucket {
		idx = append(idx, b)
	}
	slices.Sort(idx)
	out := make([]Bucket, 0, len(idx))
	for _, b := range idx {
		w := perBucket[b]
		out = append(out, Bucket{Index: b, StartS: float64(b) * BucketSize.Seconds(), Count: len(w),
			P50MS: quantile(w, 0.5), P99MS: quantile(w, 0.99)})
	}
	return out
}

// maxBacklog is the most jobs of one shop that were enqueued and not yet
// terminal at any instant before end. At equal times an enqueue counts before
// a completion, so the maximum is never understated.
func maxBacklog(jobs []Job, end time.Time) int {
	type event struct {
		at    time.Time
		delta int
	}
	var ev []event
	for _, j := range jobs {
		if !j.EnqueuedAt.Before(end) {
			continue
		}
		ev = append(ev, event{j.EnqueuedAt, +1})
		if j.TerminalAt != nil && j.TerminalAt.Before(end) {
			ev = append(ev, event{*j.TerminalAt, -1})
		}
	}
	slices.SortFunc(ev, func(a, b event) int {
		if c := a.at.Compare(b.at); c != 0 {
			return c
		}
		return b.delta - a.delta // +1 first
	})
	n, peak := 0, 0
	for _, e := range ev {
		n += e.delta
		peak = max(peak, n)
	}
	return peak
}

func dist(v []float64) Dist {
	if len(v) == 0 {
		return Dist{}
	}
	return Dist{Count: len(v), P50: quantile(v, 0.5), P99: quantile(v, 0.99), Max: slices.Max(v)}
}

// quantile sorts a copy of v and interpolates like Postgres's percentile_cont,
// as the read API does (design §11.1).
func quantile(v []float64, q float64) float64 {
	s := slices.Clone(v)
	slices.Sort(s)
	return round3(loadgen.Quantile(s, q))
}

func round3(x float64) float64 {
	f, _ := strconv.ParseFloat(strconv.FormatFloat(x, 'f', 3, 64), 64)
	return f
}
