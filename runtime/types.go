// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package runtime

import (
	"time"

	"github.com/m3db/m3db/ratelimit"
	xclose "github.com/m3db/m3x/close"
)

// Options is a set of runtime options.
type Options interface {
	// Validate will validate the runtime options are valid.
	Validate() error

	// SetPersistRateLimitOptions sets the persist rate limit options
	SetPersistRateLimitOptions(value ratelimit.Options) Options

	// PersistRateLimitOptions returns the persist rate limit options
	PersistRateLimitOptions() ratelimit.Options

	// SetWriteNewSeriesAsync sets whether to write new series asynchronously or not,
	// when true this essentially makes writes for new series eventually consistent
	// as after a write is finished you are not guaranteed to read it back immediately
	// due to inserts into the shard map being buffered. The write is however written
	// to the commit log before completing so it is considered durable.
	SetWriteNewSeriesAsync(value bool) Options

	// WriteNewSeriesAsync returns whether to write new series asynchronously or not,
	// when true this essentially makes writes for new series eventually consistent
	// as after a write is finished you are not guaranteed to read it back immediately
	// due to inserts into the shard map being buffered. The write is however written
	// to the commit log before completing so it is considered durable.
	WriteNewSeriesAsync() bool

	// SetWriteNewSeriesBackoffDuration sets the insert backoff duration during
	// periods of heavy insertions, this backoff helps gather larger batches
	// to insert into a shard in a single batch requiring far less write lock
	// acquisitions.
	SetWriteNewSeriesBackoffDuration(value time.Duration) Options

	// WriteNewSeriesBackoffDuration returns the insert backoff duration during
	// periods of heavy insertions, this backoff helps gather larger batches
	// to insert into a shard in a single batch requiring far less write lock
	// acquisitions.
	WriteNewSeriesBackoffDuration() time.Duration

	// SetWriteNewSeriesLimitPerShardPerSecond sets the insert rate limit per second,
	// setting to zero disables any rate limit for new series insertions. This rate
	// limit is primarily offered to defend against unintentional bursts of new
	// time series being inserted.
	SetWriteNewSeriesLimitPerShardPerSecond(value int) Options

	// WriteNewSeriesLimitPerShardPerSecond returns the insert rate limit per second,
	// setting to zero disables any rate limit for new series insertions. This rate
	// limit is primarily offered to defend against unintentional bursts of new
	// time series being inserted.
	WriteNewSeriesLimitPerShardPerSecond() int

	// SetTickSeriesBatchSize sets the batch size to process series together
	// during a tick before yielding and sleeping the per series duration
	// multiplied by the batch size.
	// The higher this value is the more variable CPU utilization will be
	// but the shorter ticks will ultimately be.
	SetTickSeriesBatchSize(value int) Options

	// TickSeriesBatchSize returns the batch size to process series together
	// during a tick before yielding and sleeping the per series duration
	// multiplied by the batch size.
	// The higher this value is the more variable CPU utilization will be
	// but the shorter ticks will ultimately be.
	TickSeriesBatchSize() int

	// SetTickPerSeriesSleepDuration sets the tick sleep per series value that
	// provides a constant duration to sleep per series at the end of processing
	// a batch of series during a background tick, this can directly effect how
	// fast a block is persisted after is rotated from the mutable series buffer
	// to a series block (since all series need to be merged/processed before a
	// persist can occur).
	SetTickPerSeriesSleepDuration(value time.Duration) Options

	// TickPerSeriesSleepDuration returns the tick sleep per series value that
	// provides a constant duration to sleep per series at the end of processing
	// a batch of series during a background tick, this can directly effect how
	// fast a block is persisted after is rotated from the mutable series buffer
	// to a series block (since all series need to be merged/processed before a
	// persist can occur).
	TickPerSeriesSleepDuration() time.Duration

	// SetTickMinimumInterval sets the minimum tick interval to run ticks, this
	// helps throttle the tick when the amount of series is low and the sleeps
	// on a per series basis is short.
	SetTickMinimumInterval(value time.Duration) Options

	// TickMinimumInterval returns the minimum tick interval to run ticks, this
	// helps throttle the tick when the amount of series is low and the sleeps
	// on a per series basis is short.
	TickMinimumInterval() time.Duration

	// SetMaxWiredBlocks sets the max blocks to keep wired; zero is used
	// to specify no limit. Wired blocks that are in the buffer, I.E are
	// being written to, cannot be unwired which means this limit is best
	// effort.
	SetMaxWiredBlocks(value int) Options

	// MaxWiredBlocks returns the max blocks to keep wired, zero is used
	// to specify no limit. Wired blocks that are in the buffer, I.E are
	// being written to, cannot be unwired which means this limit is best
	// effort.
	MaxWiredBlocks() int
}

// OptionsManager updates and supplies runtime options.
type OptionsManager interface {
	// Update updates the current runtime options.
	Update(value Options) error

	// Get returns the current values.
	Get() Options

	// RegisterListener registers a listener for updates to runtime options,
	// it will synchronously call back the listener when this method is called
	// to deliver the current set of runtime options.
	RegisterListener(l OptionsListener) xclose.SimpleCloser

	// Close closes the watcher and all descendent watches.
	Close()
}

// OptionsListener listens for updates to runtime options.
type OptionsListener interface {
	// SetRuntimeOptions is called when the listener is registered
	// and when any updates occurred passing the new runtime options.
	SetRuntimeOptions(value Options)
}
