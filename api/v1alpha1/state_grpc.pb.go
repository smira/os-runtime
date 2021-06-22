// Code generated by protoc-gen-go-grpc. DO NOT EDIT.

package v1alpha1

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// StateClient is the client API for State service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type StateClient interface {
	// Get a resource by type and ID.
	//
	// If a resource is not found, error is returned.
	Get(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*GetResponse, error)
	// List resources by type.
	List(ctx context.Context, in *ListRequest, opts ...grpc.CallOption) (State_ListClient, error)
	// Create a resource.
	//
	// If a resource already exists, Create returns an error.
	Create(ctx context.Context, in *CreateRequest, opts ...grpc.CallOption) (*CreateResponse, error)
	// Update a resource.
	//
	// If a resource doesn't exist, error is returned.
	// On update current version of resource `new` in the state should match
	// curVersion, otherwise conflict error is returned.
	Update(ctx context.Context, in *UpdateRequest, opts ...grpc.CallOption) (*UpdateResponse, error)
	// Destroy a resource.
	//
	// If a resource doesn't exist, error is returned.
	// If a resource has pending finalizers, error is returned.
	Destroy(ctx context.Context, in *DestroyRequest, opts ...grpc.CallOption) (*DestroyResponse, error)
	// Watch state of a resource by (namespace, type) or a specific resource by (namespace, type, id).
	//
	// It's fine to watch for a resource which doesn't exist yet.
	// Watch is canceled when context gets canceled.
	// Watch sends initial resource state as the very first event on the channel,
	// and then sends any updates to the resource as events.
	Watch(ctx context.Context, in *WatchRequest, opts ...grpc.CallOption) (State_WatchClient, error)
}

type stateClient struct {
	cc grpc.ClientConnInterface
}

func NewStateClient(cc grpc.ClientConnInterface) StateClient {
	return &stateClient{cc}
}

func (c *stateClient) Get(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*GetResponse, error) {
	out := new(GetResponse)
	err := c.cc.Invoke(ctx, "/cosi.resource.State/Get", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *stateClient) List(ctx context.Context, in *ListRequest, opts ...grpc.CallOption) (State_ListClient, error) {
	stream, err := c.cc.NewStream(ctx, &State_ServiceDesc.Streams[0], "/cosi.resource.State/List", opts...)
	if err != nil {
		return nil, err
	}
	x := &stateListClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type State_ListClient interface {
	Recv() (*ListResponse, error)
	grpc.ClientStream
}

type stateListClient struct {
	grpc.ClientStream
}

func (x *stateListClient) Recv() (*ListResponse, error) {
	m := new(ListResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *stateClient) Create(ctx context.Context, in *CreateRequest, opts ...grpc.CallOption) (*CreateResponse, error) {
	out := new(CreateResponse)
	err := c.cc.Invoke(ctx, "/cosi.resource.State/Create", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *stateClient) Update(ctx context.Context, in *UpdateRequest, opts ...grpc.CallOption) (*UpdateResponse, error) {
	out := new(UpdateResponse)
	err := c.cc.Invoke(ctx, "/cosi.resource.State/Update", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *stateClient) Destroy(ctx context.Context, in *DestroyRequest, opts ...grpc.CallOption) (*DestroyResponse, error) {
	out := new(DestroyResponse)
	err := c.cc.Invoke(ctx, "/cosi.resource.State/Destroy", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *stateClient) Watch(ctx context.Context, in *WatchRequest, opts ...grpc.CallOption) (State_WatchClient, error) {
	stream, err := c.cc.NewStream(ctx, &State_ServiceDesc.Streams[1], "/cosi.resource.State/Watch", opts...)
	if err != nil {
		return nil, err
	}
	x := &stateWatchClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type State_WatchClient interface {
	Recv() (*WatchResponse, error)
	grpc.ClientStream
}

type stateWatchClient struct {
	grpc.ClientStream
}

func (x *stateWatchClient) Recv() (*WatchResponse, error) {
	m := new(WatchResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// StateServer is the server API for State service.
// All implementations must embed UnimplementedStateServer
// for forward compatibility
type StateServer interface {
	// Get a resource by type and ID.
	//
	// If a resource is not found, error is returned.
	Get(context.Context, *GetRequest) (*GetResponse, error)
	// List resources by type.
	List(*ListRequest, State_ListServer) error
	// Create a resource.
	//
	// If a resource already exists, Create returns an error.
	Create(context.Context, *CreateRequest) (*CreateResponse, error)
	// Update a resource.
	//
	// If a resource doesn't exist, error is returned.
	// On update current version of resource `new` in the state should match
	// curVersion, otherwise conflict error is returned.
	Update(context.Context, *UpdateRequest) (*UpdateResponse, error)
	// Destroy a resource.
	//
	// If a resource doesn't exist, error is returned.
	// If a resource has pending finalizers, error is returned.
	Destroy(context.Context, *DestroyRequest) (*DestroyResponse, error)
	// Watch state of a resource by (namespace, type) or a specific resource by (namespace, type, id).
	//
	// It's fine to watch for a resource which doesn't exist yet.
	// Watch is canceled when context gets canceled.
	// Watch sends initial resource state as the very first event on the channel,
	// and then sends any updates to the resource as events.
	Watch(*WatchRequest, State_WatchServer) error
	mustEmbedUnimplementedStateServer()
}

// UnimplementedStateServer must be embedded to have forward compatible implementations.
type UnimplementedStateServer struct {
}

func (UnimplementedStateServer) Get(context.Context, *GetRequest) (*GetResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Get not implemented")
}
func (UnimplementedStateServer) List(*ListRequest, State_ListServer) error {
	return status.Errorf(codes.Unimplemented, "method List not implemented")
}
func (UnimplementedStateServer) Create(context.Context, *CreateRequest) (*CreateResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Create not implemented")
}
func (UnimplementedStateServer) Update(context.Context, *UpdateRequest) (*UpdateResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Update not implemented")
}
func (UnimplementedStateServer) Destroy(context.Context, *DestroyRequest) (*DestroyResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Destroy not implemented")
}
func (UnimplementedStateServer) Watch(*WatchRequest, State_WatchServer) error {
	return status.Errorf(codes.Unimplemented, "method Watch not implemented")
}
func (UnimplementedStateServer) mustEmbedUnimplementedStateServer() {}

// UnsafeStateServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to StateServer will
// result in compilation errors.
type UnsafeStateServer interface {
	mustEmbedUnimplementedStateServer()
}

func RegisterStateServer(s grpc.ServiceRegistrar, srv StateServer) {
	s.RegisterService(&State_ServiceDesc, srv)
}

func _State_Get_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(StateServer).Get(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/cosi.resource.State/Get",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(StateServer).Get(ctx, req.(*GetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _State_List_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(ListRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(StateServer).List(m, &stateListServer{stream})
}

type State_ListServer interface {
	Send(*ListResponse) error
	grpc.ServerStream
}

type stateListServer struct {
	grpc.ServerStream
}

func (x *stateListServer) Send(m *ListResponse) error {
	return x.ServerStream.SendMsg(m)
}

func _State_Create_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CreateRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(StateServer).Create(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/cosi.resource.State/Create",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(StateServer).Create(ctx, req.(*CreateRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _State_Update_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(UpdateRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(StateServer).Update(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/cosi.resource.State/Update",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(StateServer).Update(ctx, req.(*UpdateRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _State_Destroy_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DestroyRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(StateServer).Destroy(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/cosi.resource.State/Destroy",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(StateServer).Destroy(ctx, req.(*DestroyRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _State_Watch_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(WatchRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(StateServer).Watch(m, &stateWatchServer{stream})
}

type State_WatchServer interface {
	Send(*WatchResponse) error
	grpc.ServerStream
}

type stateWatchServer struct {
	grpc.ServerStream
}

func (x *stateWatchServer) Send(m *WatchResponse) error {
	return x.ServerStream.SendMsg(m)
}

// State_ServiceDesc is the grpc.ServiceDesc for State service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var State_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "cosi.resource.State",
	HandlerType: (*StateServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Get",
			Handler:    _State_Get_Handler,
		},
		{
			MethodName: "Create",
			Handler:    _State_Create_Handler,
		},
		{
			MethodName: "Update",
			Handler:    _State_Update_Handler,
		},
		{
			MethodName: "Destroy",
			Handler:    _State_Destroy_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "List",
			Handler:       _State_List_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "Watch",
			Handler:       _State_Watch_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "v1alpha1/state.proto",
}
