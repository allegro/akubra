package httphandler

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/allegro/akubra/transport"
	"github.com/allegro/akubra/types"
	"github.com/stretchr/testify/require"
)

func TestShouldBeFilteredInMaintenanceMode(t *testing.T) {
	getReq, err := http.NewRequest("GET", "", nil)
	require.NoError(t, err)
	nonMaintenanceModeTuple := transport.ResErrTuple{
		Req: getReq,
		Err: fmt.Errorf("Non maintenance error"),
	}
	result := shouldBeFilteredInMaintenanceMode(nonMaintenanceModeTuple)
	require.False(t, result)
	maintenanceModeTuple := transport.ResErrTuple{
		Req: getReq,
		Err: &types.BackendError{
			OrigErr: types.ErrorBackendMaintenance,
		},
	}
	result = shouldBeFilteredInMaintenanceMode(maintenanceModeTuple)
	require.True(t, result)

	putReq, err := http.NewRequest("PUT", "", nil)
	require.NoError(t, err)
	maintenancePutModeTuple := transport.ResErrTuple{
		Req: putReq,
		Err: &types.BackendError{
			OrigErr: types.ErrorBackendMaintenance,
		},
	}
	result = shouldBeFilteredInMaintenanceMode(maintenancePutModeTuple)
	require.False(t, result)

	deleteReq, err := http.NewRequest("DELETE", "", nil)
	require.NoError(t, err)
	maintenanceDeleteModeTuple := transport.ResErrTuple{
		Req: deleteReq,
		Err: &types.BackendError{
			OrigErr: types.ErrorBackendMaintenance,
		},
	}
	result = shouldBeFilteredInMaintenanceMode(maintenanceDeleteModeTuple)
	require.False(t, result)

}
