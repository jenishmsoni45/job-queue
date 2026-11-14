package bench

import (
	"fmt"
	"html"
	"math"
	"slices"
	"strings"
)

// Chart draws blackfriday.svg with the standard library (design §17): each
// shop's p99 queue wait per 10 s bucket of arrival over the run, FIFO and DRR
// side by side on one log-scale y axis, with the spike shaded. Shops 2–10, the
// ones the hypothesis is about, share one hue; the spike shop has the other.
// The colors are the first two slots of the dataviz reference palette, checked
// for color-vision separation and contrast in light and dark.
func Chart(fifo, drr Run) []byte {
	const (
		width, height   = 1200.0, 560.0
		top, bottom     = 112.0, 64.0
		left, right     = 76.0, 24.0
		gap             = 76.0
		labelGap        = 6.0
		minFloorMS      = 0.1
		lineKeyLength   = 18.0
		panelTitleSpace = 14.0
	)
	panelW := (width - left - right - gap) / 2
	plotTop, plotBottom := top, height-bottom

	// One y scale for both panels: whole powers of ten around the data.
	lo, hi := math.Inf(1), 0.0
	maxS := 0.0
	for _, r := range []Run{fifo, drr} {
		for _, buckets := range r.Series {
			for _, b := range buckets {
				if b.P99MS > 0 {
					lo = math.Min(lo, b.P99MS)
				}
				hi = math.Max(hi, b.P99MS)
				maxS = math.Max(maxS, b.StartS+r.BucketS)
			}
		}
	}
	if math.IsInf(lo, 1) {
		lo, hi = 1, 10
	}
	lo = math.Max(minFloorMS, math.Pow(10, math.Floor(math.Log10(lo))))
	hi = math.Max(lo*10, math.Pow(10, math.Ceil(math.Log10(hi))))
	maxS = math.Max(maxS, fifo.WarmupS+fifo.SpikeS)
	y := func(ms float64) float64 {
		ms = math.Max(ms, lo)
		f := (math.Log10(ms) - math.Log10(lo)) / (math.Log10(hi) - math.Log10(lo))
		return plotBottom - f*(plotBottom-plotTop)
	}

	var b strings.Builder
	w := func(format string, args ...any) { fmt.Fprintf(&b, format, args...) }
	w(`<svg xmlns="http://www.w3.org/2000/svg" width="%.0f" height="%.0f" viewBox="0 0 %.0f %.0f" role="img" aria-labelledby="title desc">`+"\n",
		width, height, width, height)
	w(`<title id="title">Black Friday: p99 queue wait per 10 s bucket, FIFO and DRR</title>` + "\n")
	w(`<desc id="desc">Two line charts side by side on one log-scale y axis. Each line is one shop's p99 queue wait for the jobs that arrived in each 10-second bucket. The shaded band is the spike, when %s sends at the spike rate. The tables in report.md hold the same numbers.</desc>`+"\n",
		html.EscapeString(fifo.SpikeShop))
	w(`<style>
svg { --surface: #fcfcfb; --ink: #0b0b0b; --ink2: #52514e; --muted: #898781; --grid: #e1e0d9; --axis: #c3c2b7; --band: #f0efec; --s1: #2a78d6; --s2: #eb6834; }
@media (prefers-color-scheme: dark) {
  svg { --surface: #1a1a19; --ink: #f0efec; --ink2: #c3c2b7; --muted: #898781; --grid: #2c2c2a; --axis: #383835; --band: #262624; --s1: #3987e5; --s2: #d95926; }
}
text { font-family: system-ui, -apple-system, "Segoe UI", sans-serif; fill: var(--ink2); font-size: 12px; font-variant-numeric: tabular-nums; }
.title { fill: var(--ink); font-size: 18px; font-weight: 600; }
.panel { fill: var(--ink); font-size: 14px; font-weight: 600; }
.muted { fill: var(--muted); }
.grid { stroke: var(--grid); stroke-width: 1; }
.axis { stroke: var(--axis); stroke-width: 1; }
.band { fill: var(--band); }
.line { fill: none; stroke-width: 2; stroke-linejoin: round; stroke-linecap: round; }
.others { stroke: var(--s1); stroke-opacity: 0.85; }
.spiker { stroke: var(--s2); }
.ring { fill: none; stroke: var(--surface); stroke-width: 4; stroke-linejoin: round; stroke-linecap: round; }
</style>
`)
	w(`<rect width="100%%" height="100%%" fill="var(--surface)"/>` + "\n")
	w(`<text class="title" x="%.0f" y="32">Black Friday: p99 queue wait per 10 s bucket of arrival</text>`+"\n", left)
	w(`<text x="%.0f" y="54">%s sends %s jobs/s during the shaded spike; every other shop stays at %s jobs/s. Log scale.</text>`+"\n",
		left, html.EscapeString(fifo.SpikeShop), trimFloat(fifo.SpikeRate), trimFloat(fifo.SteadyRate))
	// Legend: always present for two series, with a line key beside plain text.
	lx := left
	for _, item := range []struct{ class, label string }{
		{"others", "shops 2–10 (the hypothesis is about these)"},
		{"spiker", html.EscapeString(fifo.SpikeShop) + " (sends the spike)"},
	} {
		w(`<line class="line %s" x1="%.0f" y1="78" x2="%.0f" y2="78"/>`+"\n", item.class, lx, lx+lineKeyLength)
		w(`<text x="%.0f" y="82">%s</text>`+"\n", lx+lineKeyLength+labelGap, item.label)
		lx += lineKeyLength + labelGap + float64(len(item.label))*6.6 + 28
	}

	for p, r := range []Run{fifo, drr} {
		x0 := left + float64(p)*(panelW+gap)
		x := func(s float64) float64 { return x0 + s/maxS*panelW }
		name := "FIFO"
		if r.Scheduler == "drr" {
			name = "DRR (weighted deficit round robin)"
		}
		w(`<text class="panel" x="%.1f" y="%.1f">%s</text>`+"\n", x0, plotTop-panelTitleSpace, name)
		// The spike band, behind everything.
		w(`<rect class="band" x="%.1f" y="%.1f" width="%.1f" height="%.1f"/>`+"\n",
			x(r.WarmupS), plotTop, x(r.WarmupS+r.SpikeS)-x(r.WarmupS), plotBottom-plotTop)
		w(`<text class="muted" x="%.1f" y="%.1f">spike</text>`+"\n", x(r.WarmupS)+4, plotTop+14)
		// Gridlines and y ticks at each power of ten.
		for v := lo; v <= hi*1.0001; v *= 10 {
			w(`<line class="grid" x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f"/>`+"\n", x0, y(v), x0+panelW, y(v))
			w(`<text x="%.1f" y="%.1f" text-anchor="end">%s</text>`+"\n", x0-labelGap, y(v)+4, fmtMS(v))
		}
		// The baseline and x ticks every minute (two once the run passes 12 minutes).
		w(`<line class="axis" x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f"/>`+"\n", x0, plotBottom, x0+panelW, plotBottom)
		step := 60.0
		if maxS > 12*60 {
			step = 120
		}
		for s := 0.0; s <= maxS+0.001; s += step {
			w(`<text x="%.1f" y="%.1f" text-anchor="middle">%.0f</text>`+"\n", x(s), plotBottom+18, s/60)
		}
		w(`<text class="muted" x="%.1f" y="%.1f" text-anchor="middle">minutes since the first arrival</text>`+"\n", x0+panelW/2, plotBottom+40)

		// Shops 2–10 first, then the spike shop on top with a surface ring.
		ids := make([]string, 0, len(r.Series))
		for id := range r.Series {
			ids = append(ids, id)
		}
		slices.Sort(ids)
		for _, id := range ids {
			if id == r.SpikeShop {
				continue
			}
			w(`<g><title>%s, %s: p99 queue wait per 10 s bucket</title>%s</g>`+"\n", name, html.EscapeString(id),
				polylines(r.Series[id], r.BucketS, x, y, "line others"))
		}
		if s, ok := r.Series[r.SpikeShop]; ok {
			w(`<g><title>%s, %s: p99 queue wait per 10 s bucket</title>%s%s</g>`+"\n", name, html.EscapeString(r.SpikeShop),
				polylines(s, r.BucketS, x, y, "ring"), polylines(s, r.BucketS, x, y, "line spiker"))
		}
	}
	w(`<text class="muted" x="%.0f" y="%.0f">p99 queue wait</text>`+"\n", 12.0, plotTop-panelTitleSpace)
	w("</svg>\n")
	return []byte(b.String())
}

// polylines draws one shop's buckets, breaking the line where a bucket had no
// arrivals. Each point sits at its bucket's middle.
func polylines(buckets []Bucket, bucketS float64, x, y func(float64) float64, class string) string {
	var out strings.Builder
	var pts []string
	flush := func() {
		if len(pts) > 1 {
			fmt.Fprintf(&out, `<polyline class="%s" points="%s"/>`, class, strings.Join(pts, " "))
		} else if len(pts) == 1 {
			// A lone bucket still shows, as a dot.
			var px, py float64
			_, _ = fmt.Sscanf(pts[0], "%g,%g", &px, &py)
			fmt.Fprintf(&out, `<circle class="%s" cx="%.1f" cy="%.1f" r="2"/>`, class, px, py)
		}
		pts = pts[:0]
	}
	prev := -2
	for _, b := range buckets {
		if b.Index != prev+1 {
			flush()
		}
		pts = append(pts, fmt.Sprintf("%.1f,%.1f", x(b.StartS+bucketS/2), y(b.P99MS)))
		prev = b.Index
	}
	flush()
	return out.String()
}

func fmtMS(v float64) string {
	switch {
	case v >= 1000:
		return trimFloat(v/1000) + " s"
	default:
		return trimFloat(v) + " ms"
	}
}

func trimFloat(v float64) string {
	s := fmt.Sprintf("%.1f", v)
	return strings.TrimSuffix(s, ".0")
}
