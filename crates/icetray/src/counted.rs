//! `requests_total` counted around the gRPC service rather than in its
//! handlers (RFD 012): `Counted` wraps the generated server, so a request
//! tonic answers before any handler runs (a body that does not decode or
//! is too large, an encoding the server does not take) is counted too,
//! each request exactly once, under the `grpc-status` its client receives.
//! A trailers-only answer carries the status in its headers and is counted
//! as they leave; any other answer carries it in its trailers and is
//! counted as its body yields them.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use hyper::HeaderMap;
use hyper::body::{Body, Frame, SizeHint};
use tonic::Code;
use tonic::server::NamedService;

use crate::telemetry::{Call, Requests};

/// The header, or trailer, a gRPC answer carries its status code in.
const GRPC_STATUS: &str = "grpc-status";

/// The status code `headers` carry, a response's or its trailers'; one
/// that does not parse is `UNKNOWN`, as a client reads it.
fn grpc_status(headers: &HeaderMap) -> Option<Code> {
    headers
        .get(GRPC_STATUS)
        .map(|value| Code::from_bytes(value.as_bytes()))
}

/// One request being answered, counted in `requests` once: under the code
/// `finish` is given, or as `CANCELLED` when it drops first, which is how
/// a request ends whose client went away or whose `grpc-timeout` passed,
/// when tonic drops the call and answers `CANCELLED` itself.
#[derive(Debug)]
struct Answering {
    requests: Requests,
    call: Call,
    code: Code,
}

impl Answering {
    /// A request of `call`, not yet answered, to count in `requests`.
    fn new(requests: &Requests, call: Call) -> Self {
        Self {
            requests: requests.clone(),
            call,
            code: Code::Cancelled,
        }
    }

    /// Counts the request under `code`.
    fn finish(mut self, code: Code) {
        self.code = code;
    }
}

impl Drop for Answering {
    fn drop(&mut self) {
        self.requests.count(self.call, self.code);
    }
}

/// The gRPC service `inner` with every request of a `Call` counted in
/// `requests` under the status its client receives. Clone: tonic clones the
/// service per connection, and the clones share the handles.
#[derive(Clone, Debug)]
pub struct Counted<S> {
    inner: S,
    requests: Requests,
}

impl<S> Counted<S> {
    /// `inner`, counted in `requests`.
    pub fn new(inner: S, requests: Requests) -> Self {
        Self { inner, requests }
    }
}

impl<S: NamedService> NamedService for Counted<S> {
    const NAME: &'static str = S::NAME;
}

impl<S, B, R> tower::Service<hyper::Request<B>> for Counted<S>
where
    S: tower::Service<hyper::Request<B>, Response = hyper::Response<R>>,
    S::Future: Unpin,
{
    type Response = hyper::Response<CountedBody<R>>;
    type Error = S::Error;
    type Future = CountedFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: hyper::Request<B>) -> Self::Future {
        let answering =
            Call::of_path(request.uri().path()).map(|call| Answering::new(&self.requests, call));
        CountedFuture {
            inner: self.inner.call(request),
            answering,
        }
    }
}

/// The response of a counted request: counted when it carries its status
/// in its headers, else handed on with its body to be counted at the
/// trailers. A future dropped before it answers counts as `CANCELLED`.
#[derive(Debug)]
pub struct CountedFuture<F> {
    inner: F,
    answering: Option<Answering>,
}

impl<F, R, E> Future for CountedFuture<F>
where
    F: Future<Output = Result<hyper::Response<R>, E>> + Unpin,
{
    type Output = Result<hyper::Response<CountedBody<R>>, E>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        let response = ready!(Pin::new(&mut this.inner).poll(cx))?;
        let mut answering = this.answering.take();
        if let Some(code) = grpc_status(response.headers())
            && let Some(answered) = answering.take()
        {
            answered.finish(code);
        }
        Poll::Ready(Ok(response.map(|inner| CountedBody { inner, answering })))
    }
}

/// A counted response's body: the request is counted under the status its
/// trailers carry as they pass, or as `UNKNOWN` when the body ends without
/// them, since a client then has no status to read. A body that fails or
/// is dropped first counts as `CANCELLED`: its stream is reset.
#[derive(Debug)]
pub struct CountedBody<B> {
    inner: B,
    answering: Option<Answering>,
}

impl<B> CountedBody<B> {
    /// Counts the request under `code`, unless it is counted already.
    fn finish(&mut self, code: Code) {
        if let Some(answering) = self.answering.take() {
            answering.finish(code);
        }
    }
}

impl<B: Body + Unpin> Body for CountedBody<B> {
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = &mut *self;
        let polled = Pin::new(&mut this.inner).poll_frame(cx);
        match &polled {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(trailers) = frame.trailers_ref() {
                    this.finish(grpc_status(trailers).unwrap_or(Code::Unknown));
                }
            }
            Poll::Ready(None) => this.finish(Code::Unknown),
            Poll::Ready(Some(Err(_))) | Poll::Pending => {}
        }
        polled
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::convert::Infallible;
    use std::future::{Ready, ready};

    use bytes::Bytes;
    use futures::FutureExt as _;
    use http_body_util::BodyExt as _;
    use icetray_core::metric;
    use icetray_testkit::verify::{series_sum, series_value};
    use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
    use tower::Service as _;

    use super::*;
    use crate::telemetry::CodeLabel;

    /// A body that yields `frames` in order and then ends.
    #[derive(Debug, Default)]
    struct Frames(VecDeque<Frame<Bytes>>);

    impl Body for Frames {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
            Poll::Ready(self.0.pop_front().map(Ok))
        }
    }

    /// A service that answers every request with a response `answer` builds.
    #[derive(Clone)]
    struct Answers(fn() -> hyper::Response<Frames>);

    impl tower::Service<hyper::Request<()>> for Answers {
        type Response = hyper::Response<Frames>;
        type Error = Infallible;
        type Future = Ready<Result<Self::Response, Infallible>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _request: hyper::Request<()>) -> Self::Future {
            ready(Ok((self.0)()))
        }
    }

    /// Headers carrying `code` as a gRPC status.
    fn status(code: &'static str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(GRPC_STATUS, hyper::header::HeaderValue::from_static(code));
        headers
    }

    /// A trailers-only answer: the status in the headers, no body.
    fn trailers_only() -> hyper::Response<Frames> {
        let mut response = hyper::Response::new(Frames::default());
        *response.headers_mut() = status("13");
        response
    }

    /// An answer with a message and then trailers carrying `OK`.
    fn with_trailers() -> hyper::Response<Frames> {
        hyper::Response::new(Frames(VecDeque::from([
            Frame::data(Bytes::from_static(b"message")),
            Frame::trailers(status("0")),
        ])))
    }

    /// An answer whose body ends with no trailers.
    fn without_trailers() -> hyper::Response<Frames> {
        hyper::Response::new(Frames(VecDeque::from([Frame::data(Bytes::from_static(
            b"message",
        ))])))
    }

    /// `answer` behind `Counted`, over handles registered with a recorder
    /// of the test's own, so no other test's counts reach it.
    fn counted(answer: fn() -> hyper::Response<Frames>) -> (Counted<Answers>, PrometheusHandle) {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let requests = metrics::with_local_recorder(&recorder, Requests::register);
        (Counted::new(Answers(answer), requests), handle)
    }

    /// A request of `call`.
    fn request(call: Call) -> hyper::Request<()> {
        hyper::Request::builder()
            .uri(format!(
                "/{}/{}",
                icetray_proto::v1::icetray_server::SERVICE_NAME,
                call.method()
            ))
            .body(())
            .unwrap()
    }

    /// The count of `call` answering `code`.
    fn count(handle: &PrometheusHandle, call: Call, code: Code) -> f64 {
        series_value(
            &handle.render(),
            metric::REQUESTS_TOTAL,
            &[
                (metric::LABEL_CALL, &call.to_string()),
                (metric::LABEL_CODE, &CodeLabel(code).to_string()),
            ],
        )
        .expect("every series is registered")
    }

    /// Every request counted, whatever its call and code.
    fn counted_total(handle: &PrometheusHandle) -> f64 {
        series_sum(&handle.render(), metric::REQUESTS_TOTAL, &[])
    }

    /// A status in the headers is counted as the response leaves, before
    /// its body is read.
    #[test]
    fn test_call_counts_a_trailers_only_answer_at_its_headers() {
        let (mut service, handle) = counted(trailers_only);
        let response = service.call(request(Call::Get)).now_or_never().unwrap();
        assert_eq!(
            count(&handle, Call::Get, Code::Internal).to_bits(),
            1.0_f64.to_bits()
        );
        drop(response);
        assert_eq!(counted_total(&handle).to_bits(), 1.0_f64.to_bits());
    }

    /// A status in the trailers is counted as the body yields them, once.
    #[test]
    fn test_call_counts_an_answer_at_its_trailers() {
        let (mut service, handle) = counted(with_trailers);
        let mut body = service
            .call(request(Call::MultiGet))
            .now_or_never()
            .unwrap()
            .unwrap()
            .into_body();
        assert_eq!(counted_total(&handle).to_bits(), 0.0_f64.to_bits());
        let data = body.frame().now_or_never().flatten().unwrap().unwrap();
        assert!(data.is_data());
        assert_eq!(counted_total(&handle).to_bits(), 0.0_f64.to_bits());
        let trailers = body.frame().now_or_never().flatten().unwrap().unwrap();
        assert!(trailers.is_trailers());
        assert_eq!(
            count(&handle, Call::MultiGet, Code::Ok).to_bits(),
            1.0_f64.to_bits()
        );
        assert!(body.frame().now_or_never().flatten().is_none());
        drop(body);
        assert_eq!(counted_total(&handle).to_bits(), 1.0_f64.to_bits());
    }

    /// A request that ends before its status reaches the client is counted
    /// as `CANCELLED`, the future dropped unanswered or the body dropped
    /// before its trailers; a body that ends without trailers is `UNKNOWN`.
    #[test]
    fn test_call_counts_a_request_that_ends_without_a_status() {
        let (mut service, handle) = counted(with_trailers);
        drop(service.call(request(Call::Get)));
        assert_eq!(
            count(&handle, Call::Get, Code::Cancelled).to_bits(),
            1.0_f64.to_bits()
        );
        let response = service.call(request(Call::Get)).now_or_never().unwrap();
        drop(response);
        assert_eq!(
            count(&handle, Call::Get, Code::Cancelled).to_bits(),
            2.0_f64.to_bits()
        );

        let (mut service, handle) = counted(without_trailers);
        let mut body = service
            .call(request(Call::TableInfo))
            .now_or_never()
            .unwrap()
            .unwrap()
            .into_body();
        while body.frame().now_or_never().flatten().is_some() {}
        assert_eq!(
            count(&handle, Call::TableInfo, Code::Unknown).to_bits(),
            1.0_f64.to_bits()
        );
        drop(body);
        assert_eq!(counted_total(&handle).to_bits(), 1.0_f64.to_bits());
    }

    /// A request for a path that is no call is answered and not counted.
    #[test]
    fn test_call_counts_no_request_outside_the_contract_s_calls() {
        let (mut service, handle) = counted(trailers_only);
        let other = hyper::Request::builder()
            .uri("/grpc.health.v1.Health/Check")
            .body(())
            .unwrap();
        let response = service.call(other).now_or_never().unwrap();
        drop(response);
        assert_eq!(counted_total(&handle).to_bits(), 0.0_f64.to_bits());
    }
}
