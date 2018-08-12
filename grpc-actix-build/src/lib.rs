//! Generates Rust code from protobuf RPC service definitions

extern crate codegen;
extern crate heck;
extern crate prost_build;

use std::io;

use heck::{CamelCase, SnakeCase};
use std::path::Path;

/// [`prost_build::ServiceGenerator`] implementation for generating gRPC service code.
pub struct ServiceGenerator {
    /// Whether server code will be generated.
    server_code_enabled: bool,
    /// Whether client code will be generated.
    client_code_enabled: bool,
}

impl Default for ServiceGenerator {
    /// Returns a default instance of `ServiceGenerator`, with both server and client code
    /// generation enabled.
    fn default() -> Self {
        Self {
            server_code_enabled: true,
            client_code_enabled: true,
        }
    }
}

impl ServiceGenerator {
    /// Sets whether server code will be generated.
    ///
    /// Server code consists of a [`grpc_actix::MethodDispatch`] implementation that sends the
    /// appropriate [`actix::Message`] for an RPC based on an incoming HTTP request.
    ///
    /// By default, server code generation is enabled.
    ///
    /// [`grpc_actix::MethodDispatch`]: ../grpc_actix/trait.MethodDispatch.html
    /// [`actix::Message`]: https://docs.rs/actix/0.7/actix/trait.Message.html
    #[inline]
    pub fn server_code_enabled(&mut self, enable: bool) -> &mut Self {
        self.server_code_enabled = enable;
        self
    }

    /// Sets whether client code will be generated.
    ///
    /// Client code consists of [`actix::Handler`] implementations for RPC messages that use the
    /// [`grpc-actix`] client implementation.
    ///
    /// By default, client code generation is enabled.
    ///
    /// [`actix::Handler`]: https://docs.rs/actix/0.7/actix/trait.Handler.html
    /// [`grpc-actix`]: ../grpc_actix/index.html
    #[inline]
    pub fn client_code_enabled(&mut self, enable: bool) -> &mut Self {
        self.client_code_enabled = enable;
        self
    }

    /// Generates the [`actix::Message`] type for a service method.
    ///
    /// [`actix::Message`]: https://docs.rs/actix/0.7/actix/trait.Message.html
    fn method_message(
        scope: &mut codegen::Scope,
        service: &prost_build::Service,
        method: &prost_build::Method,
        method_name_camel: &str,
    ) {
        scope
            .new_struct(method_name_camel)
            .doc(&format!(
                "`{}::{}::{}` message data.",
                service.package, service.proto_name, method.proto_name
            )).vis("pub")
            .derive("Default")
            .field(
                "pub request",
                format!(
                    "::grpc_actix::{}Request<super::{}>",
                    if method.client_streaming {
                        "Streaming"
                    } else {
                        "Unary"
                    },
                    method.input_type,
                ),
            );

        scope
            .new_impl(method_name_camel)
            .impl_trait("::actix::Message")
            .associate_type(
                "Result",
                format!(
                    "Result<::grpc_actix::{}Response<super::{}>, ::grpc_actix::Status>",
                    if method.server_streaming {
                        "Streaming"
                    } else {
                        "Unary"
                    },
                    method.output_type,
                ),
            );
    }

    /// Generates the [`MethodDispatch`] implementation for a method for use with server code.
    ///
    /// [`MethodDispatch`]: ../grpc_actix/trait.MethodDispatch.html
    fn server_method_dispatch(
        scope: &mut codegen::Scope,
        service: &prost_build::Service,
        method: &prost_build::Method,
        method_name_camel: &str,
    ) {
        let dispatch_struct_name = format!("{}Dispatch", method_name_camel);

        scope
            .new_struct(&dispatch_struct_name)
            .doc(&format!(
                "Server-side dispatcher for `{}::{}::{}` RPC requests.",
                service.package, service.proto_name, method.proto_name
            )).vis("pub");

        scope
            .new_impl(&dispatch_struct_name)
            .generic("A")
            .impl_trait("::grpc_actix::MethodDispatch<A>")
            .bound(
                "A",
                format!("::actix::Handler<{}> + Send", method_name_camel),
            ).bound(
                "A::Context",
                format!("::actix::dev::ToEnvelope<A, {}>", method_name_camel),
            ).new_fn("dispatch")
            .arg_ref_self()
            .arg("addr", "::actix::Addr<A>")
            .arg("request", "::hyper::Request<::hyper::Body>")
            .ret("::grpc_actix::GrpcFuture<::hyper::Response<::grpc_actix::ResponsePayload>>")
            .line("use ::futures::Future;")
            .line("use ::grpc_actix::{Request, Response, Status, StatusCode};")
            .line(format!(
                "Box::new(::grpc_actix::{}Request::<super::{}>::from_http_request(request)",
                if method.client_streaming {
                    "Streaming"
                } else {
                    "Unary"
                },
                method.input_type
            )).line("    .and_then(move |req| {")
            .line(format!(
                "        addr.send({} {{ request: req }})",
                method_name_camel
            )).line("            .map_err(|e| Status::from_display(StatusCode::Internal, e))")
            .line("            .and_then(|resp| resp)")
            .line("            .and_then(Response::into_http_response)")
            .line("    })")
            .line(")");
    }

    /// Generates the main trait for RPC service message handlers.
    fn service_trait(
        scope: &mut codegen::Scope,
        service: &prost_build::Service,
        service_name_snake: &str,
    ) {
        {
            let service_trait = scope
                .new_trait(&service.name)
                .doc(&format!(
                    "Handler for `{}::{}` RPC service methods.",
                    service.package, service.proto_name
                )).vis("pub");
            for method in &service.methods {
                service_trait.parent(&format!(
                    "::actix::Handler<{}::{}>",
                    service_name_snake,
                    method.proto_name.to_camel_case()
                ));
            }
        }

        {
            let service_impl = scope.new_impl("T").generic("T").impl_trait(&service.name);
            for method in &service.methods {
                service_impl.bound(
                    "T",
                    format!(
                        "::actix::Handler<{}::{}>",
                        service_name_snake,
                        method.proto_name.to_camel_case()
                    ),
                );
            }
        }
    }

    /// Generates the support code for registering server-side RPC message dispatchers for a
    /// service.
    fn server_service_dispatch(
        scope: &mut codegen::Scope,
        service: &prost_build::Service,
        service_name_snake: &str,
    ) {
        let service_dispatch_name = format!("{}Dispatch", service.name);

        scope
            .new_struct(&service_dispatch_name)
            .doc(&format!(
                "Support for registering server-side `{}::{}` RPC request dispatchers.",
                service.package, service.proto_name
            )).vis("pub");

        let service_dispatch_func = scope
            .new_impl(&service_dispatch_name)
            .new_fn("add_to_service")
            .doc(&format!(
                "Adds method dispatchers for the `{}::{}` service to a `GrpcHyperService`.",
                service.package, service.proto_name
            )).vis("pub")
            .generic("A")
            .bound("A", format!("::actix::Actor + {} + Send", service.name))
            .arg("service", "&mut ::grpc_actix::GrpcHyperService<A>");
        for method in &service.methods {
            let method_name_camel = method.proto_name.to_camel_case();
            service_dispatch_func
                .bound(
                    "A::Context",
                    format!(
                        "::actix::dev::ToEnvelope<A, {}::{}>",
                        service_name_snake, method_name_camel
                    ),
                ).line(format!(
                    "service.add_dispatch(String::from(\"/{}.{}/{}\"), Box::new({}::{}Dispatch));",
                    service.package,
                    service.proto_name,
                    method.proto_name,
                    service_name_snake,
                    method_name_camel
                ));
        }
    }

    /// Generates the client actor and message handler types.
    fn client_service_code(
        scope: &mut codegen::Scope,
        service: &prost_build::Service,
        service_name_snake: &str,
    ) {
        let client_name = format!("{}Client", service.name);

        scope
            .new_struct(&client_name)
            .doc(&format!(
                "`{}::{}` RPC client.",
                service.package, service.proto_name
            )).vis("pub")
            .field("client", "::grpc_actix::Client");

        scope
            .new_impl(&client_name)
            .new_fn("new")
            .doc("Creates a new client for sending RPC requests to the specified server.")
            .vis("pub")
            .arg("server_scheme", "::http::uri::Scheme")
            .arg("server_authority", "::http::uri::Authority")
            .ret("Self")
            .line("Self {")
            .line("    client: ::grpc_actix::Client::new(server_scheme, server_authority),")
            .line("}");

        scope
            .new_impl(&client_name)
            .impl_trait("::actix::Actor")
            .associate_type("Context", "::actix::Context<Self>");

        for method in &service.methods {
            let method_name_camel = method.proto_name.to_camel_case();

            scope
                .new_impl(&client_name)
                .impl_trait(format!(
                    "::actix::Handler<{}::{}>",
                    service_name_snake, method_name_camel
                ))
                .associate_type(
                    "Result",
                    &format!(
                        "::actix::ResponseFuture<::grpc_actix::{}Response<{}>, ::grpc_actix::Status>",
                        if method.server_streaming {
                            "Streaming"
                        } else {
                            "Unary"
                        },
                        method.output_type
                    ),
                )
                .new_fn("handle")
                .arg_mut_self()
                .arg("msg", format!("{}::{}", service_name_snake, method_name_camel))
                .arg("_ctx", "&mut Self::Context")
                .ret("Self::Result")
                .line(format!(
                    "self.client.send(\"/{}.{}/{}\", msg.request) as Self::Result",
                    service.package, service.proto_name, method.proto_name
                ));
        }
    }

    /// Generates the service handler module and trait.
    fn service_code(
        &self,
        scope: &mut codegen::Scope,
        service: &prost_build::Service,
        service_name_snake: &str,
    ) {
        {
            let service_module_scope = scope.new_module(service_name_snake).vis("pub").scope();
            for method in &service.methods {
                // `method.name` contains the snake-case conversion, so explicitly convert the name
                // to camel-case for use as a struct type name.
                let method_name_camel = method.proto_name.to_camel_case();
                Self::method_message(service_module_scope, service, method, &method_name_camel);

                if self.server_code_enabled {
                    Self::server_method_dispatch(
                        service_module_scope,
                        service,
                        method,
                        &method_name_camel,
                    );
                }
            }
        }

        Self::service_trait(scope, service, service_name_snake);

        if self.server_code_enabled {
            Self::server_service_dispatch(scope, service, service_name_snake);
        }

        if self.client_code_enabled {
            Self::client_service_code(scope, service, service_name_snake);
        }
    }
}

impl prost_build::ServiceGenerator for ServiceGenerator {
    fn generate(&mut self, service: prost_build::Service, buf: &mut String) {
        let mut scope = codegen::Scope::new();
        let service_name_snake = service.proto_name.to_snake_case();

        self.service_code(&mut scope, &service, &service_name_snake);

        *buf += &scope.to_string();
    }
}

/// Wrapper for `prost_build::compile_protos()` that utilizes this crate's [`ServiceGenerator`] to
/// generate gRPC service code.
///
/// The default Protocol Buffers and gRPC code generation settings will be used. To adjust the
/// settings for either, users should instead create a `prost_build::Config` and
/// [`ServiceGenerator`] instance and modify each accordingly.
///
/// [`ServiceGenerator`]: struct.ServiceGenerator.html
pub fn compile_protos<P>(protos: &[P], includes: &[P]) -> io::Result<()>
where
    P: AsRef<Path>,
{
    prost_build::Config::new()
        .service_generator(Box::new(ServiceGenerator::default()))
        .compile_protos(protos, includes)
}
