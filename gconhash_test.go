/*
 MIT License
 Copyright (c) 2019 Max Kuznetsov <syhpoon@syhpoon.ca>
 Permission is hereby granted, free of charge, to any person obtaining a copy
 of this software and associated documentation files (the "Software"), to deal
 in the Software without restriction, including without limitation the rights
 to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the Software is
 furnished to do so, subject to the following conditions:
 The above copyright notice and this permission notice shall be included in all
 copies or substantial portions of the Software.
 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 SOFTWARE.
*/

package gconhash

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHasher(t *testing.T) {
	// host1 = 0                   - 2049638230412172401,
	//         2049638230412172401 - 4099276460824344802,
	//         14347467612885206807 - 16397105843297379208
	// host2 = 4099276460824344802 - 6148914691236517203,
	//         8198552921648689604 - 10248191152060862005,
	//         12297829382473034406 - 14347467612885206807
	// host3 = 6148914691236517203 - 8198552921648689604,
	//         10248191152060862005 - 12297829382473034406,
	//         16397105843297379208 - 18446744073709551609

	// key1  = 8161715635210842401  -> host3
	// key2  = 1911516019731155366  -> host1
	// key3  = 3139388526329846568  -> host1
	// key4  = 15878513814679191950 -> host1
	// key8  = 4793660841873507130  -> host2
	// key75 = 11301308226069127036 -> host3

	hasher := New([]string{
		"host1",
		"host2",
		"host3",
	}, 9, 10)

	require.Equal(t, "host3", hasher.IdForKey("key1"))
	require.Equal(t, "host1", hasher.IdForKey("key2"))
	require.Equal(t, "host1", hasher.IdForKey("key3"))
	require.Equal(t, "host1", hasher.IdForKey("key4"))
	require.Equal(t, "host2", hasher.IdForKey("key8"))
	require.Equal(t, "host3", hasher.IdForKey("key75"))
}
