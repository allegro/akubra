package privacy

import (
	"context"
	"fmt"

	"net/http"

	"github.com/allegro/akubra/internal/akubra/log"
)

const (
	//RequestPrivacyContextKey is the key under which PrivacyContext can be found in context.Context
	RequestPrivacyContextKey = log.ContextKey("RequestPrivacyContextKey")
)

//Context holds the privacy settings associated with the request
type Context struct {
	isInternalNetwork bool
}

//ContextSupplier supplies the context.Context of req with a privacy context
type ContextSupplier interface {
	Supply(req *http.Request) (*http.Request, error)
}

//Config is a configuration for ContextSupplier
type Config struct {
	IsInternalNetworkHeaderName  string `yaml:"IsInternalNetworkHeaderName"`
	IsInternalNetworkHeaderValue string `yaml:"IsInternalNetworkHeaderValue"`
	ShouldDropRequests           bool   `yaml:"ShouldDropRequests"`
	ViolationErrorCode           int    `yaml:"ViolationErrorCode"`
}

//BasicPrivacyContextSupplier is a basic implemtation of ContextSupplier
type BasicPrivacyContextSupplier struct {
	config *Config
}

//NewBasicPrivacyContextSupplier creates an instance of BasicPrivacyContextSupplier
func NewBasicPrivacyContextSupplier(config *Config) ContextSupplier {
	return &BasicPrivacyContextSupplier{config: config}
}

//Supply supplies the request with basic privacy info
func (basicSupplier *BasicPrivacyContextSupplier) Supply(req *http.Request) (*http.Request, error) {
	headerValue := req.Header.Get(basicSupplier.config.IsInternalNetworkHeaderName)
	isInternalNetwork := headerValue == basicSupplier.config.IsInternalNetworkHeaderValue
	privacyContext := &Context{
		isInternalNetwork: isInternalNetwork,
	}
	contextWithPrivacy := context.WithValue(req.Context(), RequestPrivacyContextKey, privacyContext)
	return req.WithContext(contextWithPrivacy), nil
}

//SupplierRoundTripper supplies the request with a privacy context using the given Supplier
type SupplierRoundTripper struct {
	roundTripper           http.RoundTripper
	privacyContextSupplier ContextSupplier
}

//NewPrivacyContextSupplierRoundTripper creates an instance of SupplierRoundTripper
func NewPrivacyContextSupplierRoundTripper(roundTripper http.RoundTripper, supplier ContextSupplier) http.RoundTripper {
	return &SupplierRoundTripper{
		roundTripper:           roundTripper,
		privacyContextSupplier: supplier,
	}
}

//RoundTrip supplies the request with basic privacy info
func (supplierRT *SupplierRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	reqWithPrivacy, err := supplierRT.privacyContextSupplier.Supply(req)
	if err != nil {
		reqID := req.Context().Value(log.ContextreqIDKey).(string)
		return nil, fmt.Errorf("failed to supply request %s with privacy context, reason: %s", reqID, err)
	}
	return supplierRT.roundTripper.RoundTrip(reqWithPrivacy)
}
