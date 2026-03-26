package util

import "time"

type Clock interface {
	Now() time.Time
	After(time.Duration) <-chan time.Time
}

type RealClock struct{}

func (RealClock) Now() time.Time {
	return time.Now().UTC()
}

func (RealClock) After(d time.Duration) <-chan time.Time {
	return time.After(d)
}

func FormatTimestamp(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000Z")
}
