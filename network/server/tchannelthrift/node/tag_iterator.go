// Copyright (c) 2018 Uber Technologies, Inc.
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

package node

import (
	"github.com/m3db/m3db/client"
	"github.com/m3db/m3x/ident"
)

// TODO(prateek): migrate to using m3x
// TODO(prateek): rethink ownerwhip semantics of Tag, and Tags
// TODO(prateek): rethink tagIter.Clone() semantics

// NewTagSliceIterator returns a TagIterator over a slice.
func newTagSliceIterator(tags ident.Tags, p client.TagArrayPool) ident.TagIterator {
	iter := &tagSliceIter{
		backingSlice: tags,
		currentIdx:   -1,
		pool:         p,
	}
	return iter
}

type tagSliceIter struct {
	backingSlice ident.Tags
	currentIdx   int
	currentTag   ident.Tag
	pool         client.TagArrayPool
}

func (i *tagSliceIter) Next() bool {
	i.currentIdx++
	if i.currentIdx < len(i.backingSlice) {
		i.currentTag = i.backingSlice[i.currentIdx]
		return true
	}
	i.currentTag = ident.Tag{}
	return false
}

func (i *tagSliceIter) Current() ident.Tag {
	return i.currentTag
}

func (i *tagSliceIter) Err() error {
	return nil
}

func (i *tagSliceIter) Close() {
	if arr := i.backingSlice; arr != nil {
		if i.pool != nil {
			i.pool.Put(arr)
		}
	}
	i.backingSlice = nil
	i.currentIdx = 0
	i.currentTag = ident.Tag{}
}

func (i *tagSliceIter) Remaining() int {
	if r := len(i.backingSlice) - 1 - i.currentIdx; r >= 0 {
		return r
	}
	return 0
}

func (i *tagSliceIter) Clone() ident.TagIterator {
	return &tagSliceIter{
		backingSlice: i.backingSlice,
		currentIdx:   i.currentIdx,
		currentTag:   i.currentTag,
		// TODO(prateek): better to use the reset semantics than cloning :|
		// pool:         i.pool.,
	}
}
