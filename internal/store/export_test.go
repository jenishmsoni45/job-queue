package store

// CandidatesSQL and SnapshotShopsSQL let the plan test check the statements the
// claim and the dispatcher's refresh run.
func CandidatesSQL(shopCount int) string { return candidatesSQL(shopCount) }

const SnapshotShopsSQL = snapshotShopsSQL
