// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package inmem

import (
	"context"
	"time"
)

// StateOptions configure inmem.State.
type StateOptions struct {
	BackingStore BackingStore

	// HistoryCleanupCtx bounds the lifetime of the history cleanup goroutine, it is nil if the
	// cleanup is disabled.
	HistoryCleanupCtx context.Context //nolint:containedctx

	HistoryCleanupInterval time.Duration
	HistoryMaxCapacity     int
	HistoryInitialCapacity int
	HistoryGap             int
}

// StateOption applies settings to StateOptions.
type StateOption func(options *StateOptions)

// WithHistoryCapacity sets history depth of the state event buffer.
//
// Deprecated: use WithHistoryMaxCapacity and WithHistoryInitialCapacity instead.
func WithHistoryCapacity(capacity int) StateOption {
	return func(options *StateOptions) {
		options.HistoryMaxCapacity = capacity
		options.HistoryInitialCapacity = capacity
	}
}

// WithHistoryMaxCapacity sets history depth of the state event buffer.
//
// The event buffer is shared by all namespaces and resource types of the state.
//
// Deep history requires more memory, but allows Watch request to return more historical entries, and also
// acts like a buffer if watch consumer can't keep up with events.
//
// Max capacity limits the maximum depth of the history buffer.
func WithHistoryMaxCapacity(maxCapacity int) StateOption {
	return func(options *StateOptions) {
		options.HistoryMaxCapacity = maxCapacity

		if options.HistoryInitialCapacity > options.HistoryMaxCapacity {
			options.HistoryInitialCapacity = options.HistoryMaxCapacity
		}
	}
}

// WithHistoryInitialCapacity sets initial history depth of the state event buffer.
//
// The event buffer is shared by all namespaces and resource types of the state.
//
// Deep history requires more memory, but allows Watch request to return more historical entries, and also
// acts like a buffer if watch consumer can't keep up with events.
//
// Initial capacity of the history buffer is used at the creation time and grows to the max capacity
// based on the number of events.
func WithHistoryInitialCapacity(initialCapacity int) StateOption {
	return func(options *StateOptions) {
		options.HistoryInitialCapacity = initialCapacity

		if options.HistoryMaxCapacity < options.HistoryInitialCapacity {
			options.HistoryMaxCapacity = options.HistoryInitialCapacity
		}
	}
}

// WithHistoryGap sets a safety gap between watch events consumers and events producers.
//
// Bigger gap reduces effective history depth (HistoryCapacity - HistoryGap).
// Smaller gap might result in buffer overruns if consumer can't keep up with the events.
// It's recommended to have gap 5% of the capacity.
func WithHistoryGap(gap int) StateOption {
	return func(options *StateOptions) {
		options.HistoryGap = gap
	}
}

// WithHistoryCleanup enables the background cleanup of the state event history buffer.
//
// The event history buffer grows up to the max capacity as the events are published, and it never
// shrinks back. An event held by the buffer keeps the resources it references alive, so a state
// which went through a burst of changes keeps the resources of that burst in memory long after the
// consumers are done with them.
//
// The cleanup releases the events which every watcher of their resource type has already consumed
// and which are older than the interval, so that the resources become garbage. The buffer capacity
// itself is not affected, only the memory the events reference.
//
// The cleanup shortens the effective history: a watch which starts from a bookmark pointing to a
// released event fails with an error matching state.IsInvalidWatchBookmarkError, and TailEvents
// returns only the events which are still retained. Callers which resume from bookmarks should
// already handle that error by restarting the watch from scratch, as the same happens when the
// events are pushed out of the buffer.
//
// The cleanup runs until the context is canceled. Default is no cleanup.
func WithHistoryCleanup(ctx context.Context, interval time.Duration) StateOption {
	return func(options *StateOptions) {
		options.HistoryCleanupCtx = ctx
		options.HistoryCleanupInterval = interval
	}
}

// WithBackingStore sets a BackingStore for a in-memory resource collection.
//
// Default value is nil (no backing store).
func WithBackingStore(store BackingStore) StateOption {
	return func(options *StateOptions) {
		options.BackingStore = store
	}
}

// DefaultStateOptions returns default value of StateOptions.
//
// As the history buffer is shared by all namespaces and resource types of the state, the default
// capacity is much bigger than the depth which used to be reserved per resource type.
func DefaultStateOptions() StateOptions {
	return StateOptions{
		HistoryMaxCapacity:     40960,
		HistoryInitialCapacity: 256,
		HistoryGap:             50,
	}
}
