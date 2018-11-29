package watchdog

import (

"net/http"

"github.com/stretchr/testify/mock"

)

type ConsistencyRecordFactoryMock struct {
	*mock.Mock
}

func (fm *ConsistencyRecordFactoryMock) CreateRecordFor(request *http.Request) (*ConsistencyRecord, error) {
	args := fm.Called(request)
	record := args.Get(0).(*ConsistencyRecord)
	err := args.Error(1)
	return record, err
}

