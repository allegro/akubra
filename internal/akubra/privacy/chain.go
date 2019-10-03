package privacy

import (
	"errors"
	"fmt"
	"github.com/allegro/akubra/internal/akubra/log"
	"github.com/allegro/akubra/internal/akubra/metrics"
	"net/http"
	"sync/atomic"
	"time"
)

//ViolationType is an code indiciating which (if any) privacy policy has been violated
type ViolationType int

const (
	//NoViolation means that no violations have been deteced
	NoViolation ViolationType = iota
	//InternalNetworkBucket means that access to internal-network-only bucket has been requested
	//from an external network
	InternalNetworkBucket
)

//ErrPrivacyContextNotPresent indicates that the privacy.Context is not present in request's context.Context
var ErrPrivacyContextNotPresent = errors.New("privacy context not present")

//Chain runs a set of filters on a req to determine wether any privacy policies have been violated
type Chain interface {
	Filter(req *http.Request) (ViolationType, error)
}

//ChainRoundTripper uses the supplied Chain to determine any privacy policy violations
type ChainRoundTripper struct {
	roundTripper          http.RoundTripper
	chain                 Chain
	shouldDropOnViolation bool
	violationsCount       int64
}

//NewChainRoundTripper creates an instance of ChainRoundTripper
func NewChainRoundTripper(shouldDrop bool, chain Chain, roundTripper http.RoundTripper) http.RoundTripper {
	chainRT := &ChainRoundTripper{
		roundTripper:          roundTripper,
		chain:                 chain,
		shouldDropOnViolation: shouldDrop,
	}
	go chainRT.reportMetrics()
	return chainRT
}

//RoundTrip checks for violations on req
func (chainRT *ChainRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	reqID := req.Context().Value(log.ContextreqIDKey).(string)
	violation, err := chainRT.chain.Filter(req)
	if err != nil {
		violationCheckErr := fmt.Errorf("failed to filter req %s: %s", reqID, err)
		if chainRT.shouldDropOnViolation {
			return nil, violationCheckErr
		}
		log.Debug(violationCheckErr)
	}

	if violation == NoViolation {
		return chainRT.roundTripper.RoundTrip(req)
	}

	log.Printf("detected violation of type %d on req %s", violation, reqID)
	atomic.AddInt64(&chainRT.violationsCount, 1)

	if chainRT.shouldDropOnViolation {
		return violationDetectedFor(req), nil
	}

	return chainRT.roundTripper.RoundTrip(req)
}

func violationDetectedFor(req *http.Request) *http.Response {
	return &http.Response{
		StatusCode: http.StatusForbidden,
		Status:     "Privacy policy violated",
		Proto:      req.Proto,
		ProtoMajor: req.ProtoMajor,
		ProtoMinor: req.ProtoMinor,
		Request:    req,
	}
}

func (chainRT *ChainRoundTripper) reportMetrics() {
	for {
		metrics.UpdateGauge("privacy.violation", chainRT.violationsCount)
		atomic.SwapInt64(&chainRT.violationsCount, 0)
		time.Sleep(10 * time.Second)
	}
}

//Filter is a funcion that check requests for a specific violation
type Filter = func(req *http.Request, prvCtx *Context) (ViolationType, error)

//BasicChain runs each of the filters on the request until the first violation/errror is returned
type BasicChain struct {
	filters []Filter
}

//NewBasicChain creates an instance of BasicChain
func NewBasicChain(filters []Filter) Chain {
	return &BasicChain{
		filters: filters,
	}
}

//Filter checks for privacy context presence and runs the supplied filters on the req
func (basicChain *BasicChain) Filter(req *http.Request) (ViolationType, error) {
	privacyContext, castOK := req.Context().Value(RequestPrivacyContextKey).(*Context)
	if !castOK || privacyContext == nil {
		return NoViolation, ErrPrivacyContextNotPresent
	}

	for _, filter := range basicChain.filters {
		violation, err := filter(req, privacyContext)
		if violation != NoViolation || err != nil {
			return violation, err
		}
	}

	return NoViolation, nil
}
