// +build integration_disabled

// Copyright (c) 2016 Uber Technologies, Inc.
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

package integration

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

// This test writes a larget number of unique series' with tags concurrently.
func TestIndexLargeCardinalityHighConcurrency(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	concurrency := 2
	writeEach := 2
	maxNumTags := 10

	genIDTags := func(i int, j int) (ident.ID, ident.TagIterator) {
		id := fmt.Sprintf("foo.%d.%d", i, j)
		numTags := rand.Intn(maxNumTags)
		tags := make([]ident.Tag, 0, numTags)
		for i := 0; i < numTags; i++ {
			tags = append(tags, ident.StringTag(
				fmt.Sprintf("%s.tagname.%d", id, i),
				fmt.Sprintf("%s.tagvalue.%d", id, i),
			))
		}
		return ident.StringID(id), ident.NewTagSliceIterator(tags)
	}

	// Test setup
	testOpts := newTestOptions(t) // .SetIndexingEnabled(true)
	testSetup, err := newTestSetup(t, testOpts, nil)
	require.NoError(t, err)
	defer testSetup.close()

	md := testSetup.namespaceMetadataOrFail(testNamespaces[0])

	// Start the server
	log := testSetup.storageOpts.InstrumentOptions().Logger()
	require.NoError(t, testSetup.startServer())

	// Stop the server
	defer func() {
		require.NoError(t, testSetup.stopServer())
		log.Debug("server is now down")
	}()

	var wg sync.WaitGroup
	now := time.Now()
	log.Info("starting data write")

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		idx := 0
		go func() {
			log.Infof("starting write %d", idx)
			ctx := context.NewContext()
			defer ctx.BlockingClose()
			for j := 0; j < writeEach; j++ {
				id, tags := genIDTags(idx, j)
				err := testSetup.db.WriteTagged(ctx, md.ID(), id, tags, now, float64(1.0), xtime.Second, nil)
				require.NoError(t, err)
			}
			log.Infof("finishing write %d", idx)
			wg.Done()
		}()
	}

	wg.Wait()
	log.Infof("test data written in %v", time.Since(now))
}