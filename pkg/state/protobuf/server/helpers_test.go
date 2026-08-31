// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package server_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/cosi-project/runtime/api/v1alpha1"
	"github.com/cosi-project/runtime/pkg/state/protobuf/server"
)

func TestConvertIDQuery(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name        string
		regexp      string
		expectedErr string
		expectedNil bool
	}{
		{
			name:        "empty",
			regexp:      "",
			expectedNil: true,
		},
		{
			name:   "simple",
			regexp: "^foo-[0-9]+$",
		},
		{
			name:        "invalid",
			regexp:      "^foo-[0-9+$",
			expectedErr: "rpc error: code = InvalidArgument desc = failed to parse regexp: error parsing regexp: missing closing ]: `[0-9+$`",
		},
		{
			name:        "too long",
			regexp:      strings.Repeat("a", 4097),
			expectedErr: "rpc error: code = InvalidArgument desc = regexp is too long: 4097 > 4096",
		},
		{
			// short pattern, but expands to ~1M instructions and ~200MiB when compiled
			name:        "too complex",
			regexp:      "(" + strings.Repeat("a", 1024) + "){1000}",
			expectedErr: "rpc error: code = InvalidArgument desc = regexp is too complex: 1026000 > 4096",
		},
		{
			name:        "too complex, nested repeats",
			regexp:      "((abcdefghij){10}){100}",
			expectedErr: "rpc error: code = InvalidArgument desc = regexp is too complex: 12200 > 4096",
		},
		{
			name:   "just under the complexity limit",
			regexp: "(" + strings.Repeat("a", 7) + "){450}",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			opts, err := server.ConvertIDQuery(&v1alpha1.IDQuery{Regexp: test.regexp})

			if test.expectedErr != "" {
				require.Error(t, err)
				assert.Equal(t, test.expectedErr, err.Error())
				assert.Nil(t, opts)

				return
			}

			require.NoError(t, err)

			if test.expectedNil {
				assert.Nil(t, opts)
			} else {
				assert.Len(t, opts, 1)
			}
		})
	}
}

func TestConvertIDQueryNil(t *testing.T) {
	t.Parallel()

	opts, err := server.ConvertIDQuery(nil)
	require.NoError(t, err)
	assert.Nil(t, opts)
}

func TestConvertLabelQueryMissingValue(t *testing.T) {
	t.Parallel()

	for _, op := range []v1alpha1.LabelTerm_Operation{
		v1alpha1.LabelTerm_EQUAL,
		v1alpha1.LabelTerm_LT,
		v1alpha1.LabelTerm_LTE,
		v1alpha1.LabelTerm_LT_NUMERIC,
		v1alpha1.LabelTerm_LTE_NUMERIC,
	} {
		t.Run(op.String(), func(t *testing.T) {
			t.Parallel()

			opts, err := server.ConvertLabelQuery([]*v1alpha1.LabelTerm{{Op: op, Key: "key"}})
			require.Error(t, err)
			assert.Equal(t, "rpc error: code = InvalidArgument desc = missing value for label query operator: "+op.String(), err.Error())
			assert.Nil(t, opts)
		})
	}
}

func TestConvertLabelQuery(t *testing.T) {
	t.Parallel()

	// operators which don't require a value
	for _, op := range []v1alpha1.LabelTerm_Operation{
		v1alpha1.LabelTerm_EXISTS,
		v1alpha1.LabelTerm_NOT_EXISTS,
		v1alpha1.LabelTerm_IN,
	} {
		t.Run(op.String(), func(t *testing.T) {
			t.Parallel()

			opts, err := server.ConvertLabelQuery([]*v1alpha1.LabelTerm{{Op: op, Key: "key"}})
			require.NoError(t, err)
			assert.Len(t, opts, 1)
		})
	}

	t.Run("with values", func(t *testing.T) {
		t.Parallel()

		opts, err := server.ConvertLabelQuery([]*v1alpha1.LabelTerm{
			{Op: v1alpha1.LabelTerm_EQUAL, Key: "key", Value: []string{"value"}},
			{Op: v1alpha1.LabelTerm_LTE_NUMERIC, Key: "key", Value: []string{"5"}, Invert: true},
		})
		require.NoError(t, err)
		assert.Len(t, opts, 2)
	})

	t.Run("missing values", func(t *testing.T) {
		t.Parallel()

		_, err := server.ConvertLabelQuery([]*v1alpha1.LabelTerm{
			{Op: v1alpha1.LabelTerm_EQUAL, Key: "key"},
		})
		require.Error(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("unsupported operator", func(t *testing.T) {
		t.Parallel()

		opts, err := server.ConvertLabelQuery([]*v1alpha1.LabelTerm{{Op: v1alpha1.LabelTerm_Operation(100), Key: "key"}})
		require.Error(t, err)
		assert.Equal(t, "rpc error: code = Unimplemented desc = unsupported label query operator: 100", err.Error())
		assert.Nil(t, opts)
	})
}
