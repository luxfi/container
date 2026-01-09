// Copyright (C) 2019-2025, Lux Industries, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package iterator_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/luxfi/ids"
	"github.com/luxfi/container/iterator"
)

func TestMerge(t *testing.T) {
	type test struct {
		name      string
		iterators []iterator.Iterator[*testStaker]
		expected  []*testStaker
	}

	txID := ids.GenerateTestID()
	tests := []test{
		{
			name:      "no iterators",
			iterators: []iterator.Iterator[*testStaker]{},
			expected:  []*testStaker{},
		},
		{
			name:      "one empty iterator",
			iterators: []iterator.Iterator[*testStaker]{iterator.Empty[*testStaker]{}},
			expected:  []*testStaker{},
		},
		{
			name:      "multiple empty iterator",
			iterators: []iterator.Iterator[*testStaker]{iterator.Empty[*testStaker]{}, iterator.Empty[*testStaker]{}, iterator.Empty[*testStaker]{}},
			expected:  []*testStaker{},
		},
		{
			name:      "mixed empty iterators",
			iterators: []iterator.Iterator[*testStaker]{iterator.Empty[*testStaker]{}, iterator.FromSlice[*testStaker]()},
			expected:  []*testStaker{},
		},
		{
			name: "single iterator",
			iterators: []iterator.Iterator[*testStaker]{
				iterator.FromSlice[*testStaker](
					&testStaker{
						TxID:     txID,
						NextTime: time.Unix(0, 0),
					},
					&testStaker{
						TxID:     txID,
						NextTime: time.Unix(1, 0),
					},
				),
			},
			expected: []*testStaker{
				{
					TxID:     txID,
					NextTime: time.Unix(0, 0),
				},
				{
					TxID:     txID,
					NextTime: time.Unix(1, 0),
				},
			},
		},
		{
			name: "multiple iterators",
			iterators: []iterator.Iterator[*testStaker]{
				iterator.FromSlice[*testStaker](
					&testStaker{
						TxID:     txID,
						NextTime: time.Unix(0, 0),
					},
					&testStaker{
						TxID:     txID,
						NextTime: time.Unix(2, 0),
					},
				),
				iterator.FromSlice[*testStaker](
					&testStaker{
						TxID:     txID,
						NextTime: time.Unix(1, 0),
					},
					&testStaker{
						TxID:     txID,
						NextTime: time.Unix(3, 0),
					},
				),
			},
			expected: []*testStaker{
				{
					TxID:     txID,
					NextTime: time.Unix(0, 0),
				},
				{
					TxID:     txID,
					NextTime: time.Unix(1, 0),
				},
				{
					TxID:     txID,
					NextTime: time.Unix(2, 0),
				},
				{
					TxID:     txID,
					NextTime: time.Unix(3, 0),
				},
			},
		},
		{
			name: "multiple iterators different lengths",
			iterators: []iterator.Iterator[*testStaker]{
				iterator.FromSlice[*testStaker](
					&testStaker{
						TxID:     txID,
						NextTime: time.Unix(0, 0),
					},
					&testStaker{
						TxID:     txID,
						NextTime: time.Unix(2, 0),
					},
				),
				iterator.FromSlice[*testStaker](
					&testStaker{
						TxID:     txID,
						NextTime: time.Unix(1, 0),
					},
					&testStaker{
						TxID:     txID,
						NextTime: time.Unix(3, 0),
					},
					&testStaker{
						TxID:     txID,
						NextTime: time.Unix(4, 0),
					},
					&testStaker{
						TxID:     txID,
						NextTime: time.Unix(5, 0),
					},
				),
			},
			expected: []*testStaker{
				{
					TxID:     txID,
					NextTime: time.Unix(0, 0),
				},
				{
					TxID:     txID,
					NextTime: time.Unix(1, 0),
				},
				{
					TxID:     txID,
					NextTime: time.Unix(2, 0),
				},
				{
					TxID:     txID,
					NextTime: time.Unix(3, 0),
				},
				{
					TxID:     txID,
					NextTime: time.Unix(4, 0),
				},
				{
					TxID:     txID,
					NextTime: time.Unix(5, 0),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			it := iterator.Merge[*state.Staker]((*state.Staker).Less, tt.iterators...)
			for _, expected := range tt.expected {
				require.True(it.Next())
				require.Equal(expected, it.Value())
			}
			require.False(it.Next())
			it.Release()
			require.False(it.Next())
		})
	}
}

func TestMergedEarlyRelease(t *testing.T) {
	require := require.New(t)
	stakers0 := []*state.Staker{
		{
			TxID:     ids.GenerateTestID(),
			NextTime: time.Unix(0, 0),
		},
		{
			TxID:     ids.GenerateTestID(),
			NextTime: time.Unix(2, 0),
		},
	}

	stakers1 := []*state.Staker{
		{
			TxID:     ids.GenerateTestID(),
			NextTime: time.Unix(1, 0),
		},
		{
			TxID:     ids.GenerateTestID(),
			NextTime: time.Unix(3, 0),
		},
	}

	it := iterator.Merge(
		(*state.Staker).Less,
		iterator.Empty[*state.Staker]{},
		iterator.FromSlice(stakers0...),
		iterator.Empty[*state.Staker]{},
		iterator.FromSlice(stakers1...),
		iterator.Empty[*state.Staker]{},
	)
	require.True(it.Next())
	it.Release()
	require.False(it.Next())
}
