#![allow(unused)]

mod interlude {
    pub use api_utils_rs::{api, prelude::*};

    pub use crate::{Context, SharedContext};
    pub use async_trait::async_trait;
}

use crate::interlude::*;
use api_utils_rs::api;
use futures::TryFutureExt;

pub struct Context {
    config: Config,
    db: api::StdDb,
    // kanidm: kanidm_client::KanidmClient,
    // rt: tokio::runtime::Runtime,
}

pub type SharedContext = Arc<Context>;

#[derive(educe::Educe, Clone)]
#[educe(Deref, DerefMut)]
pub struct ServiceContext(pub SharedContext);

#[derive(educe::Educe, Clone)]
#[educe(Deref, DerefMut)]
pub struct SharedServiceContext(pub ServiceContext);

#[derive(Debug)]
pub struct Config {}

// mod bindings;
mod doc;
mod gen;

fn init() -> Res<()> {
    // CX.set(Arc::new(Context {
    //     config: Config {},
    //     db: StdDb::PgWasi {},
    //     // rt: tokio::runtime::Builder::new_current_thread()
    //     //     .enable_all()
    //     //     .build()
    //     //     .wrap_err(ERROR_TOKIO)?,
    // }))
    // .map_err(|_| ferr!("double component intialization"))?;
    Ok(())
}

fn cx() -> SharedContext {
    crate::CX
        .get_or_init(|| {
            Arc::new(Context {
                config: Config {},
                db: StdDb::PgWasi {},
                // rt: tokio::runtime::Builder::new_current_thread()
                //     .enable_all()
                //     .build()
                //     .wrap_err(ERROR_TOKIO)?,
            })
        })
        // .expect("component was not initialized")
        .clone()
}

pub const CX: std::sync::OnceLock<SharedContext> = std::sync::OnceLock::new();

mod wit {
    #[allow(unfulfilled_lint_expectations, unused_imports)]
    use api_utils_rs::wit::utils as __with_name9;
    #[allow(unfulfilled_lint_expectations, unused_imports)]
    use api_utils_rs::wit::wasi::clocks::monotonic_clock as __with_name1;
    #[allow(unfulfilled_lint_expectations, unused_imports)]
    use api_utils_rs::wit::wasi::clocks::wall_clock as __with_name2;
    #[allow(unfulfilled_lint_expectations, unused_imports)]
    use api_utils_rs::wit::wasi::config::runtime as __with_name3;
    #[allow(unfulfilled_lint_expectations, unused_imports)]
    use api_utils_rs::wit::wasi::io::poll as __with_name0;
    #[allow(unfulfilled_lint_expectations, unused_imports)]
    use api_utils_rs::wit::wasi::keyvalue::atomics as __with_name5;
    #[allow(unfulfilled_lint_expectations, unused_imports)]
    use api_utils_rs::wit::wasi::keyvalue::store as __with_name4;
    #[allow(unfulfilled_lint_expectations, unused_imports)]
    use api_utils_rs::wit::wasi::logging::logging as __with_name6;
    #[allow(unfulfilled_lint_expectations, unused_imports)]
    use api_utils_rs::wit::wasmcloud::postgres::query as __with_name8;
    #[allow(unfulfilled_lint_expectations, unused_imports)]
    use api_utils_rs::wit::wasmcloud::postgres::types as __with_name7;
    #[allow(dead_code, clippy::all)]
    pub mod townframe {
        pub mod daybook_api {
            #[allow(dead_code, async_fn_in_trait, unused_imports, clippy::all)]
            pub mod doc {
                #[used]
                #[doc(hidden)]
                static __FORCE_SECTION_REF: fn() =
                    super::super::super::__link_custom_section_describing_imports;
                use super::super::super::_rt;
                pub type Datetime = super::super::super::__with_name9::Datetime;
                pub type Mutlihash = _rt::String;
            }
        }
    }
    #[allow(dead_code, clippy::all)]
    pub mod exports {
        pub mod townframe {
            pub mod daybook_api {
                #[allow(dead_code, async_fn_in_trait, unused_imports, clippy::all)]
                pub mod ctx {
                    #[used]
                    #[doc(hidden)]
                    static __FORCE_SECTION_REF: fn() =
                        super::super::super::super::__link_custom_section_describing_imports;
                    use super::super::super::super::_rt;
                    #[doc(hidden)]
                    #[allow(non_snake_case, unused_unsafe)]
                    pub unsafe fn _export_init_cabi<T: Guest>() -> *mut u8 {
                        unsafe {
                            #[cfg(target_arch = "wasm32")]
                            _rt::run_ctors_once();
                            let result0 = { T::init() };
                            let ptr1 = (&raw mut _RET_AREA.0).cast::<u8>();
                            match result0 {
                                Ok(_) => {
                                    *ptr1.add(0).cast::<u8>() = (0i32) as u8;
                                }
                                Err(e) => {
                                    *ptr1.add(0).cast::<u8>() = (1i32) as u8;
                                    let vec2 = (e.into_bytes()).into_boxed_slice();
                                    let ptr2 = vec2.as_ptr().cast::<u8>();
                                    let len2 = vec2.len();
                                    ::core::mem::forget(vec2);
                                    *ptr1
                                        .add(2 * ::core::mem::size_of::<*const u8>())
                                        .cast::<usize>() = len2;
                                    *ptr1
                                        .add(::core::mem::size_of::<*const u8>())
                                        .cast::<*mut u8>() = ptr2.cast_mut();
                                }
                            };
                            ptr1
                        }
                    }
                    #[doc(hidden)]
                    #[allow(non_snake_case)]
                    pub unsafe fn __post_return_init<T: Guest>(arg0: *mut u8) {
                        unsafe {
                            let l0 = i32::from(*arg0.add(0).cast::<u8>());
                            match l0 {
                                0 => (),
                                _ => {
                                    let l1 = *arg0
                                        .add(::core::mem::size_of::<*const u8>())
                                        .cast::<*mut u8>();
                                    let l2 = *arg0
                                        .add(2 * ::core::mem::size_of::<*const u8>())
                                        .cast::<usize>();
                                    _rt::cabi_dealloc(l1, l2, 1);
                                }
                            }
                        }
                    }
                    pub trait Guest {
                        #[allow(async_fn_in_trait)]
                        fn init() -> Result<(), _rt::String>;
                    }
                    #[doc(hidden)]
                    macro_rules! __export_townframe_daybook_api_ctx_cabi {
                    ($ty:ident with_types_in$($path_to_types:tt)*) => (const _:() = {
                        #[unsafe(export_name = "townframe:daybook-api/ctx#init")]unsafe extern "C" fn export_init()-> *mut u8 {
                            unsafe {
                                $($path_to_types)*::_export_init_cabi::<$ty>()
                            }
                        }#[unsafe(export_name = "cabi_post_townframe:daybook-api/ctx#init")]unsafe extern "C" fn _post_return_init(arg0: *mut u8,){
                            unsafe {
                                $($path_to_types)*::__post_return_init::<$ty>(arg0)
                            }
                        }
                    };
                    );
                }
                    #[doc(hidden)]
                    pub(crate) use __export_townframe_daybook_api_ctx_cabi;
                    #[cfg_attr(target_pointer_width = "64", repr(align(8)))]
                    #[cfg_attr(target_pointer_width = "32", repr(align(4)))]
                    struct _RetArea(
                        [::core::mem::MaybeUninit<u8>; 3 * ::core::mem::size_of::<*const u8>()],
                    );

                    static mut _RET_AREA: _RetArea = _RetArea(
                        [::core::mem::MaybeUninit::uninit();
                            3 * ::core::mem::size_of::<*const u8>()],
                    );
                }
                #[allow(dead_code, async_fn_in_trait, unused_imports, clippy::all)]
                pub mod doc_create {
                    #[used]
                    #[doc(hidden)]
                    static __FORCE_SECTION_REF: fn() =
                        super::super::super::super::__link_custom_section_describing_imports;
                    use super::super::super::super::_rt;
                    pub type ErrorsValidation =
                        super::super::super::super::__with_name9::ErrorsValidation;
                    pub type ErrorInternal =
                        super::super::super::super::__with_name9::ErrorInternal;
                    pub type Uuid = super::super::super::super::__with_name9::Uuid;
                    pub type Doc = crate::gen::doc::Doc;
                    #[doc = " Id occupied"]
                    #[doc = " http error code: BAD_REQUEST"]
                    #[derive(Clone, serde::Deserialize, serde::Serialize)]
                    pub struct ErrorIdOccupied {
                        pub id: _rt::String,
                    }
                    impl ::core::fmt::Debug for ErrorIdOccupied {
                        fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                            f.debug_struct("ErrorIdOccupied")
                                .field("id", &self.id)
                                .finish()
                        }
                    }
                    #[derive(Clone, serde::Deserialize, serde::Serialize)]
                    pub enum Error {
                        IdOccupied(crate::gen::doc::doc_create::ErrorIdOccupied),
                        InvalidInput(ErrorsValidation),
                        Internal(ErrorInternal),
                    }
                    impl ::core::fmt::Debug for Error {
                        fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                            match self {
                                Error::IdOccupied(e) => {
                                    f.debug_tuple("Error::IdOccupied").field(e).finish()
                                }
                                Error::InvalidInput(e) => {
                                    f.debug_tuple("Error::InvalidInput").field(e).finish()
                                }
                                Error::Internal(e) => {
                                    f.debug_tuple("Error::Internal").field(e).finish()
                                }
                            }
                        }
                    }
                    impl ::core::fmt::Display for Error {
                        fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                            write!(f, "{:?}", self)
                        }
                    }
                    impl ::core::error::Error for Error {}

                    #[derive(Clone, serde::Deserialize, serde::Serialize)]
                    pub struct Input {
                        pub id: Uuid,
                    }
                    impl ::core::fmt::Debug for Input {
                        fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                            f.debug_struct("Input").field("id", &self.id).finish()
                        }
                    }
                    pub type Output = crate::gen::doc::Doc;
                    #[derive(Debug)]
                    #[repr(transparent)]
                    pub struct Service {
                        handle: _rt::Resource<Service>,
                    }
                    type _ServiceRep<T> = Option<T>;
                    impl Service {
                        #[doc = " Creates a new resource from the specified representation."]
                        #[doc = ""]
                        #[doc = " This function will create a new resource handle by moving `val` onto"]
                        #[doc = " the heap and then passing that heap pointer to the component model to"]
                        #[doc = " create a handle. The owned handle is then returned as `Service`."]
                        pub fn new<T: GuestService>(val: T) -> Self {
                            Self::type_guard::<T>();
                            let val: _ServiceRep<T> = Some(val);
                            let ptr: *mut _ServiceRep<T> = _rt::Box::into_raw(_rt::Box::new(val));
                            unsafe { Self::from_handle(T::_resource_new(ptr.cast())) }
                        }
                        #[doc = " Gets access to the underlying `T` which represents this resource."]
                        pub fn get<T: GuestService>(&self) -> &T {
                            let ptr = unsafe { &*self.as_ptr::<T>() };
                            ptr.as_ref().unwrap()
                        }
                        #[doc = " Gets mutable access to the underlying `T` which represents this"]
                        #[doc = " resource."]
                        pub fn get_mut<T: GuestService>(&mut self) -> &mut T {
                            let ptr = unsafe { &mut *self.as_ptr::<T>() };
                            ptr.as_mut().unwrap()
                        }
                        #[doc = " Consumes this resource and returns the underlying `T`."]
                        pub fn into_inner<T: GuestService>(self) -> T {
                            let ptr = unsafe { &mut *self.as_ptr::<T>() };
                            ptr.take().unwrap()
                        }
                        #[doc(hidden)]
                        pub unsafe fn from_handle(handle: u32) -> Self {
                            Self {
                                handle: unsafe { _rt::Resource::from_handle(handle) },
                            }
                        }
                        #[doc(hidden)]
                        pub fn take_handle(&self) -> u32 {
                            _rt::Resource::take_handle(&self.handle)
                        }
                        #[doc(hidden)]
                        pub fn handle(&self) -> u32 {
                            _rt::Resource::handle(&self.handle)
                        }
                        #[doc(hidden)]
                        fn type_guard<T: 'static>() {
                            use core::any::TypeId;
                            static mut LAST_TYPE: Option<TypeId> = None;
                            unsafe {
                                assert!(!cfg!(target_feature = "atomics"));
                                let id = TypeId::of::<T>();
                                match LAST_TYPE {
                                    Some(ty) => {
                                        assert!(ty == id,"cannot use two types with this resource type")
                                    }
                                    None => LAST_TYPE = Some(id),
                                }
                            }
                        }
                        #[doc(hidden)]
                        pub unsafe fn dtor<T: 'static>(handle: *mut u8) {
                            Self::type_guard::<T>();
                            let _ = unsafe { _rt::Box::from_raw(handle as *mut _ServiceRep<T>) };
                        }
                        fn as_ptr<T: GuestService>(&self) -> *mut _ServiceRep<T> {
                            Service::type_guard::<T>();
                            T::_resource_rep(self.handle()).cast()
                        }
                    }
                    #[doc = " A borrowed version of [`Service`] which represents a borrowed value"]
                    #[doc = " with the lifetime `\'a`."]
                    #[derive(Debug)]
                    #[repr(transparent)]
                    pub struct ServiceBorrow<'a> {
                        rep: *mut u8,
                        _marker: core::marker::PhantomData<&'a Service>,
                    }
                    impl<'a> ServiceBorrow<'a> {
                        #[doc(hidden)]
                        pub unsafe fn lift(rep: usize) -> Self {
                            Self {
                                rep: rep as *mut u8,
                                _marker: core::marker::PhantomData,
                            }
                        }
                        #[doc = " Gets access to the underlying `T` in this resource."]
                        pub fn get<T: GuestService>(&self) -> &'a T {
                            let ptr = unsafe { &mut *self.as_ptr::<T>() };
                            ptr.as_ref().unwrap()
                        }
                        fn as_ptr<T: 'static>(&self) -> *mut _ServiceRep<T> {
                            Service::type_guard::<T>();
                            self.rep.cast()
                        }
                    }
                    unsafe impl _rt::WasmResource for Service {
                        #[inline]
                        unsafe fn drop(_handle: u32) {
                            #[cfg(target_arch = "wasm32")]
                            #[link(wasm_import_module = "[export]townframe:daybook-api/doc-create")]
                            unsafe extern "C" {
                                #[link_name = "[resource-drop]service"]
                                fn drop(_: i32);

                            }
                            #[cfg(not(target_arch = "wasm32"))]
                            unsafe extern "C" fn drop(_: i32) {
                                unreachable!()
                            }
                            unsafe {
                                drop(_handle as i32);
                            }
                        }
                    }
                    #[doc(hidden)]
                    #[allow(non_snake_case, unused_unsafe)]
                    pub unsafe fn _export_constructor_service_cabi<T: GuestService>() -> i32 {
                        unsafe {
                            #[cfg(target_arch = "wasm32")]
                            _rt::run_ctors_once();
                            let result0 = { Service::new(T::new()) };
                            (result0).take_handle() as i32
                        }
                    }
                    #[doc(hidden)]
                    #[allow(non_snake_case, unused_unsafe)]
                    pub unsafe fn _export_method_service_serve_cabi<T: GuestService>(
                        arg0: *mut u8,
                        arg1: *mut u8,
                        arg2: usize,
                    ) -> *mut u8 {
                        unsafe {
                            #[cfg(target_arch = "wasm32")]
                            _rt::run_ctors_once();
                            let result1 = {
                                let len0 = arg2;
                                let bytes0 = _rt::Vec::from_raw_parts(arg1.cast(), len0, len0);
                                T::serve(
                                    ServiceBorrow::lift(arg0 as u32 as usize).get(),
                                    crate::gen::doc::doc_create::Input {
                                        id: _rt::string_lift(bytes0),
                                    },
                                )
                            };
                            let ptr2 = (&raw mut _RET_AREA.0).cast::<u8>();
                            match result1 {
                                Ok(e) => {
                                    *ptr2.add(0).cast::<u8>() = (0i32) as u8;
                                    let crate::gen::doc::Doc {
                                        id: id3,
                                        created_at: created_at3,
                                        updated_at: updated_at3,
                                        content: content3,
                                        props: tags3,
                                    } = e;
                                    let vec4 = (id3.into_bytes()).into_boxed_slice();
                                    let ptr4 = vec4.as_ptr().cast::<u8>();
                                    let len4 = vec4.len();
                                    ::core::mem::forget(vec4);
                                    *ptr2
                                        .add(8 + 1 * ::core::mem::size_of::<*const u8>())
                                        .cast::<usize>() = len4;
                                    *ptr2.add(8).cast::<*mut u8>() = ptr4.cast_mut();
                                    let super::super::super::super::__with_name2::Datetime {
                                        seconds: seconds5,
                                        nanoseconds: nanoseconds5,
                                    } = created_at3;
                                    *ptr2
                                        .add(8 + 2 * ::core::mem::size_of::<*const u8>())
                                        .cast::<i64>() = _rt::as_i64(seconds5);
                                    *ptr2
                                        .add(16 + 2 * ::core::mem::size_of::<*const u8>())
                                        .cast::<i32>() = _rt::as_i32(nanoseconds5);
                                    let super::super::super::super::__with_name2::Datetime {
                                        seconds: seconds6,
                                        nanoseconds: nanoseconds6,
                                    } = updated_at3;
                                    *ptr2
                                        .add(24 + 2 * ::core::mem::size_of::<*const u8>())
                                        .cast::<i64>() = _rt::as_i64(seconds6);
                                    *ptr2
                                        .add(32 + 2 * ::core::mem::size_of::<*const u8>())
                                        .cast::<i32>() = _rt::as_i32(nanoseconds6);
                                    *ptr2
                                        .add(40 + 2 * ::core::mem::size_of::<*const u8>())
                                        .cast::<u8>() = (content3.clone() as i32) as u8;
                                    let vec10 = tags3;
                                    let len10 = vec10.len();
                                    let layout10 = _rt::alloc::Layout::from_size_align(
                                        vec10.len() * (3 * ::core::mem::size_of::<*const u8>()),
                                        ::core::mem::size_of::<*const u8>(),
                                    )
                                    .unwrap();
                                    let (result10, _cleanup10) =
                                        wit_bindgen::rt::Cleanup::new(layout10);
                                    if let Some(cleanup) = _cleanup10 {
                                        cleanup.forget();
                                    }
                                    for (i, e) in vec10.into_iter().enumerate() {
                                        let base = result10
                                            .add(i * (3 * ::core::mem::size_of::<*const u8>()));
                                        {
                                            use daybook_types::DocProp as V9;
                                            match e {
                                                V9::RefGeneric(e) => {
                                                    *base.add(0).cast::<u8>() = (0i32) as u8;
                                                    let vec7 = (e.into_bytes()).into_boxed_slice();
                                                    let ptr7 = vec7.as_ptr().cast::<u8>();
                                                    let len7 = vec7.len();
                                                    ::core::mem::forget(vec7);
                                                    *base
                                                        .add(
                                                            2 * ::core::mem::size_of::<*const u8>(),
                                                        )
                                                        .cast::<usize>() = len7;
                                                    *base
                                                        .add(::core::mem::size_of::<*const u8>())
                                                        .cast::<*mut u8>() = ptr7.cast_mut();
                                                }
                                                V9::LabelGeneric(e) => {
                                                    *base.add(0).cast::<u8>() = (1i32) as u8;
                                                    let vec8 = (e.into_bytes()).into_boxed_slice();
                                                    let ptr8 = vec8.as_ptr().cast::<u8>();
                                                    let len8 = vec8.len();
                                                    ::core::mem::forget(vec8);
                                                    *base
                                                        .add(
                                                            2 * ::core::mem::size_of::<*const u8>(),
                                                        )
                                                        .cast::<usize>() = len8;
                                                    *base
                                                        .add(::core::mem::size_of::<*const u8>())
                                                        .cast::<*mut u8>() = ptr8.cast_mut();
                                                }
                                            }
                                        }
                                    }
                                    *ptr2
                                        .add(40 + 4 * ::core::mem::size_of::<*const u8>())
                                        .cast::<usize>() = len10;
                                    *ptr2
                                        .add(40 + 3 * ::core::mem::size_of::<*const u8>())
                                        .cast::<*mut u8>() = result10;
                                }
                                Err(e) => {
                                    *ptr2.add(0).cast::<u8>() = (1i32) as u8;
                                    use crate::gen::doc::doc_create::Error as V20;
                                    match e {
                                        V20::IdOccupied(e) => {
                                            *ptr2.add(8).cast::<u8>() = (0i32) as u8;
                                            let crate::gen::doc::doc_create::ErrorIdOccupied {
                                                id: id11,
                                            } = e;
                                            let vec12 = (id11.into_bytes()).into_boxed_slice();
                                            let ptr12 = vec12.as_ptr().cast::<u8>();
                                            let len12 = vec12.len();
                                            ::core::mem::forget(vec12);
                                            *ptr2
                                                .add(8 + 2 * ::core::mem::size_of::<*const u8>())
                                                .cast::<usize>() = len12;
                                            *ptr2
                                                .add(8 + 1 * ::core::mem::size_of::<*const u8>())
                                                .cast::<*mut u8>() = ptr12.cast_mut();
                                        }
                                        V20::InvalidInput(e) => {
                                            *ptr2.add(8).cast::<u8>() = (1i32) as u8;
                                            let super::super::super::super::__with_name9::ErrorsValidation {
                                                issues:issues13,
                                            } = e;
                                            let vec17 = issues13;
                                            let len17 = vec17.len();
                                            let layout17 = _rt::alloc::Layout::from_size_align(
                                                vec17.len()
                                                    * (4 * ::core::mem::size_of::<*const u8>()),
                                                ::core::mem::size_of::<*const u8>(),
                                            )
                                            .unwrap();
                                            let (result17, _cleanup17) =
                                                wit_bindgen::rt::Cleanup::new(layout17);
                                            if let Some(cleanup) = _cleanup17 {
                                                cleanup.forget();
                                            }
                                            for (i, e) in vec17.into_iter().enumerate() {
                                                let base = result17.add(
                                                    i * (4 * ::core::mem::size_of::<*const u8>()),
                                                );
                                                {
                                                    let (t14_0, t14_1) = e;
                                                    let vec15 =
                                                        (t14_0.into_bytes()).into_boxed_slice();
                                                    let ptr15 = vec15.as_ptr().cast::<u8>();
                                                    let len15 = vec15.len();
                                                    ::core::mem::forget(vec15);
                                                    *base
                                                        .add(::core::mem::size_of::<*const u8>())
                                                        .cast::<usize>() = len15;
                                                    *base.add(0).cast::<*mut u8>() =
                                                        ptr15.cast_mut();
                                                    let vec16 =
                                                        (t14_1.into_bytes()).into_boxed_slice();
                                                    let ptr16 = vec16.as_ptr().cast::<u8>();
                                                    let len16 = vec16.len();
                                                    ::core::mem::forget(vec16);
                                                    *base
                                                        .add(
                                                            3 * ::core::mem::size_of::<*const u8>(),
                                                        )
                                                        .cast::<usize>() = len16;
                                                    *base
                                                        .add(
                                                            2 * ::core::mem::size_of::<*const u8>(),
                                                        )
                                                        .cast::<*mut u8>() = ptr16.cast_mut();
                                                }
                                            }
                                            *ptr2
                                                .add(8 + 2 * ::core::mem::size_of::<*const u8>())
                                                .cast::<usize>() = len17;
                                            *ptr2
                                                .add(8 + 1 * ::core::mem::size_of::<*const u8>())
                                                .cast::<*mut u8>() = result17;
                                        }
                                        V20::Internal(e) => {
                                            *ptr2.add(8).cast::<u8>() = (2i32) as u8;
                                            let super::super::super::super::__with_name9::ErrorInternal {
                                                message:message18,
                                            } = e;
                                            let vec19 = (message18.into_bytes()).into_boxed_slice();
                                            let ptr19 = vec19.as_ptr().cast::<u8>();
                                            let len19 = vec19.len();
                                            ::core::mem::forget(vec19);
                                            *ptr2
                                                .add(8 + 2 * ::core::mem::size_of::<*const u8>())
                                                .cast::<usize>() = len19;
                                            *ptr2
                                                .add(8 + 1 * ::core::mem::size_of::<*const u8>())
                                                .cast::<*mut u8>() = ptr19.cast_mut();
                                        }
                                    }
                                }
                            };
                            ptr2
                        }
                    }
                    #[doc(hidden)]
                    #[allow(non_snake_case)]
                    pub unsafe fn __post_return_method_service_serve<T: GuestService>(
                        arg0: *mut u8,
                    ) {
                        unsafe {
                            let l0 = i32::from(*arg0.add(0).cast::<u8>());
                            match l0 {
                                0 => {
                                    let l1 = *arg0.add(8).cast::<*mut u8>();
                                    let l2 = *arg0
                                        .add(8 + 1 * ::core::mem::size_of::<*const u8>())
                                        .cast::<usize>();
                                    _rt::cabi_dealloc(l1, l2, 1);
                                    let l3 = *arg0
                                        .add(40 + 3 * ::core::mem::size_of::<*const u8>())
                                        .cast::<*mut u8>();
                                    let l4 = *arg0
                                        .add(40 + 4 * ::core::mem::size_of::<*const u8>())
                                        .cast::<usize>();
                                    let base10 = l3;
                                    let len10 = l4;
                                    for i in 0..len10 {
                                        let base = base10
                                            .add(i * (3 * ::core::mem::size_of::<*const u8>()));
                                        {
                                            let l5 = i32::from(*base.add(0).cast::<u8>());
                                            match l5 {
                                                0 => {
                                                    let l6 = *base
                                                        .add(::core::mem::size_of::<*const u8>())
                                                        .cast::<*mut u8>();
                                                    let l7 = *base
                                                        .add(
                                                            2 * ::core::mem::size_of::<*const u8>(),
                                                        )
                                                        .cast::<usize>();
                                                    _rt::cabi_dealloc(l6, l7, 1);
                                                }
                                                _ => {
                                                    let l8 = *base
                                                        .add(::core::mem::size_of::<*const u8>())
                                                        .cast::<*mut u8>();
                                                    let l9 = *base
                                                        .add(
                                                            2 * ::core::mem::size_of::<*const u8>(),
                                                        )
                                                        .cast::<usize>();
                                                    _rt::cabi_dealloc(l8, l9, 1);
                                                }
                                            }
                                        }
                                    }
                                    _rt::cabi_dealloc(
                                        base10,
                                        len10 * (3 * ::core::mem::size_of::<*const u8>()),
                                        ::core::mem::size_of::<*const u8>(),
                                    );
                                }
                                _ => {
                                    let l11 = i32::from(*arg0.add(8).cast::<u8>());
                                    match l11 {
                                        0 => {
                                            let l12 = *arg0
                                                .add(8 + 1 * ::core::mem::size_of::<*const u8>())
                                                .cast::<*mut u8>();
                                            let l13 = *arg0
                                                .add(8 + 2 * ::core::mem::size_of::<*const u8>())
                                                .cast::<usize>();
                                            _rt::cabi_dealloc(l12, l13, 1);
                                        }
                                        1 => {
                                            let l14 = *arg0
                                                .add(8 + 1 * ::core::mem::size_of::<*const u8>())
                                                .cast::<*mut u8>();
                                            let l15 = *arg0
                                                .add(8 + 2 * ::core::mem::size_of::<*const u8>())
                                                .cast::<usize>();
                                            let base20 = l14;
                                            let len20 = l15;
                                            for i in 0..len20 {
                                                let base = base20.add(
                                                    i * (4 * ::core::mem::size_of::<*const u8>()),
                                                );
                                                {
                                                    let l16 = *base.add(0).cast::<*mut u8>();
                                                    let l17 = *base
                                                        .add(::core::mem::size_of::<*const u8>())
                                                        .cast::<usize>();
                                                    _rt::cabi_dealloc(l16, l17, 1);
                                                    let l18 = *base
                                                        .add(
                                                            2 * ::core::mem::size_of::<*const u8>(),
                                                        )
                                                        .cast::<*mut u8>();
                                                    let l19 = *base
                                                        .add(
                                                            3 * ::core::mem::size_of::<*const u8>(),
                                                        )
                                                        .cast::<usize>();
                                                    _rt::cabi_dealloc(l18, l19, 1);
                                                }
                                            }
                                            _rt::cabi_dealloc(
                                                base20,
                                                len20 * (4 * ::core::mem::size_of::<*const u8>()),
                                                ::core::mem::size_of::<*const u8>(),
                                            );
                                        }
                                        _ => {
                                            let l21 = *arg0
                                                .add(8 + 1 * ::core::mem::size_of::<*const u8>())
                                                .cast::<*mut u8>();
                                            let l22 = *arg0
                                                .add(8 + 2 * ::core::mem::size_of::<*const u8>())
                                                .cast::<usize>();
                                            _rt::cabi_dealloc(l21, l22, 1);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    pub trait Guest {
                        type Service: GuestService;
                    }
                    pub trait GuestService: 'static {
                        #[doc(hidden)]
                        unsafe fn _resource_new(val: *mut u8) -> u32
                        where
                            Self: Sized,
                        {
                            #[cfg(target_arch = "wasm32")]
                            #[link(wasm_import_module = "[export]townframe:daybook-api/doc-create")]
                            unsafe extern "C" {
                                #[link_name = "[resource-new]service"]
                                fn new(_: *mut u8) -> i32;

                            }
                            #[cfg(not(target_arch = "wasm32"))]
                            unsafe extern "C" fn new(_: *mut u8) -> i32 {
                                unreachable!()
                            }
                            unsafe { new(val) as u32 }
                        }
                        #[doc(hidden)]
                        fn _resource_rep(handle: u32) -> *mut u8
                        where
                            Self: Sized,
                        {
                            #[cfg(target_arch = "wasm32")]
                            #[link(wasm_import_module = "[export]townframe:daybook-api/doc-create")]
                            unsafe extern "C" {
                                #[link_name = "[resource-rep]service"]
                                fn rep(_: i32) -> *mut u8;

                            }
                            #[cfg(not(target_arch = "wasm32"))]
                            unsafe extern "C" fn rep(_: i32) -> *mut u8 {
                                unreachable!()
                            }
                            unsafe { rep(handle as i32) }
                        }
                        #[allow(async_fn_in_trait)]
                        fn new() -> Self;

                        #[allow(async_fn_in_trait)]
                        fn serve(
                            &self,
                            inp: crate::gen::doc::doc_create::Input,
                        ) -> Result<crate::gen::doc::Doc, crate::gen::doc::doc_create::Error>;
                    }
                    #[doc(hidden)]
                    macro_rules! __export_townframe_daybook_api_doc_create_cabi {
                    ($ty:ident with_types_in$($path_to_types:tt)*) => (const _:() = {
                        #[unsafe(export_name = "townframe:daybook-api/doc-create#[constructor]service")]unsafe extern "C" fn export_constructor_service()->i32 {
                            unsafe {
                                $($path_to_types)*::_export_constructor_service_cabi::<<$ty as $($path_to_types)*::Guest>::Service>()
                            }
                        }#[unsafe(export_name = "townframe:daybook-api/doc-create#[method]service.serve")]unsafe extern "C" fn export_method_service_serve(arg0: *mut u8,arg1: *mut u8,arg2:usize,)-> *mut u8 {
                            unsafe {
                                $($path_to_types)*::_export_method_service_serve_cabi::<<$ty as $($path_to_types)*::Guest>::Service>(arg0,arg1,arg2)
                            }
                        }#[unsafe(export_name = "cabi_post_townframe:daybook-api/doc-create#[method]service.serve")]unsafe extern "C" fn _post_return_method_service_serve(arg0: *mut u8,){
                            unsafe {
                                $($path_to_types)*::__post_return_method_service_serve::<<$ty as $($path_to_types)*::Guest>::Service>(arg0)
                            }
                        }const _:() = {
                            #[doc(hidden)]#[unsafe(export_name = "townframe:daybook-api/doc-create#[dtor]service")]#[allow(non_snake_case)]unsafe extern "C" fn dtor(rep: *mut u8){
                                unsafe {
                                    $($path_to_types)*::Service::dtor::< <$ty as $($path_to_types)*::Guest>::Service>(rep)
                                }
                            }
                        };
                    };
                    );
                }
                    #[doc(hidden)]
                    pub(crate) use __export_townframe_daybook_api_doc_create_cabi;
                    #[repr(align(8))]
                    struct _RetArea(
                        [::core::mem::MaybeUninit<u8>;
                            48 + 4 * ::core::mem::size_of::<*const u8>()],
                    );

                    static mut _RET_AREA: _RetArea = _RetArea(
                        [::core::mem::MaybeUninit::uninit();
                            48 + 4 * ::core::mem::size_of::<*const u8>()],
                    );
                }
            }
        }
    }
    mod _rt {
        #![allow(dead_code, clippy::all)]
        pub use alloc_crate::string::String;
        #[cfg(target_arch = "wasm32")]
        pub fn run_ctors_once() {
            wit_bindgen::rt::run_ctors_once();
        }
        pub unsafe fn cabi_dealloc(ptr: *mut u8, size: usize, align: usize) {
            if size == 0 {
                return;
            }
            unsafe {
                let layout = alloc::Layout::from_size_align_unchecked(size, align);
                alloc::dealloc(ptr, layout);
            }
        }
        use core::fmt;
        use core::marker;
        use core::sync::atomic::{AtomicU32, Ordering::Relaxed};
        #[doc = " A type which represents a component model resource, either imported or"]
        #[doc = " exported into this component."]
        #[doc = ""]
        #[doc = " This is a low-level wrapper which handles the lifetime of the resource"]
        #[doc = " (namely this has a destructor). The `T` provided defines the component model"]
        #[doc = " intrinsics that this wrapper uses."]
        #[doc = ""]
        #[doc = " One of the chief purposes of this type is to provide `Deref` implementations"]
        #[doc = " to access the underlying data when it is owned."]
        #[doc = ""]
        #[doc = " This type is primarily used in generated code for exported and imported"]
        #[doc = " resources."]
        #[repr(transparent)]
        pub struct Resource<T: WasmResource> {
            handle: AtomicU32,
            _marker: marker::PhantomData<T>,
        }
        #[doc = " A trait which all wasm resources implement, namely providing the ability to"]
        #[doc = " drop a resource."]
        #[doc = ""]
        #[doc = " This generally is implemented by generated code, not user-facing code."]
        #[allow(clippy::missing_safety_doc)]
        pub unsafe trait WasmResource {
            #[doc = " Invokes the `[resource-drop]...` intrinsic."]
            unsafe fn drop(handle: u32);
        }
        impl<T: WasmResource> Resource<T> {
            #[doc(hidden)]
            pub unsafe fn from_handle(handle: u32) -> Self {
                debug_assert!(handle != 0 && handle != u32::MAX);
                Self {
                    handle: AtomicU32::new(handle),
                    _marker: marker::PhantomData,
                }
            }
            #[doc = " Takes ownership of the handle owned by `resource`."]
            #[doc = ""]
            #[doc = " Note that this ideally would be `into_handle` taking `Resource<T>` by"]
            #[doc = " ownership. The code generator does not enable that in all situations,"]
            #[doc = " unfortunately, so this is provided instead."]
            #[doc = ""]
            #[doc = " Also note that `take_handle` is in theory only ever called on values"]
            #[doc = " owned by a generated function. For example a generated function might"]
            #[doc = " take `Resource<T>` as an argument but then call `take_handle` on a"]
            #[doc = " reference to that argument. In that sense the dynamic nature of"]
            #[doc = " `take_handle` should only be exposed internally to generated code, not"]
            #[doc = " to user code."]
            #[doc(hidden)]
            pub fn take_handle(resource: &Resource<T>) -> u32 {
                resource.handle.swap(u32::MAX, Relaxed)
            }
            #[doc(hidden)]
            pub fn handle(resource: &Resource<T>) -> u32 {
                resource.handle.load(Relaxed)
            }
        }
        impl<T: WasmResource> fmt::Debug for Resource<T> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_struct("Resource")
                    .field("handle", &self.handle)
                    .finish()
            }
        }
        impl<T: WasmResource> Drop for Resource<T> {
            fn drop(&mut self) {
                unsafe {
                    match self.handle.load(Relaxed) {
                        u32::MAX => {}

                        other => T::drop(other),
                    }
                }
            }
        }
        pub use alloc_crate::boxed::Box;
        pub use alloc_crate::vec::Vec;
        pub unsafe fn string_lift(bytes: Vec<u8>) -> String {
            if cfg!(debug_assertions) {
                String::from_utf8(bytes).unwrap()
            } else {
                unsafe { String::from_utf8_unchecked(bytes) }
            }
        }
        pub fn as_i64<T: AsI64>(t: T) -> i64 {
            t.as_i64()
        }
        pub trait AsI64 {
            fn as_i64(self) -> i64;
        }
        impl<'a, T: Copy + AsI64> AsI64 for &'a T {
            fn as_i64(self) -> i64 {
                (*self).as_i64()
            }
        }
        impl AsI64 for i64 {
            #[inline]
            fn as_i64(self) -> i64 {
                self as i64
            }
        }
        impl AsI64 for u64 {
            #[inline]
            fn as_i64(self) -> i64 {
                self as i64
            }
        }
        pub fn as_i32<T: AsI32>(t: T) -> i32 {
            t.as_i32()
        }
        pub trait AsI32 {
            fn as_i32(self) -> i32;
        }
        impl<'a, T: Copy + AsI32> AsI32 for &'a T {
            fn as_i32(self) -> i32 {
                (*self).as_i32()
            }
        }
        impl AsI32 for i32 {
            #[inline]
            fn as_i32(self) -> i32 {
                self as i32
            }
        }
        impl AsI32 for u32 {
            #[inline]
            fn as_i32(self) -> i32 {
                self as i32
            }
        }
        impl AsI32 for i16 {
            #[inline]
            fn as_i32(self) -> i32 {
                self as i32
            }
        }
        impl AsI32 for u16 {
            #[inline]
            fn as_i32(self) -> i32 {
                self as i32
            }
        }
        impl AsI32 for i8 {
            #[inline]
            fn as_i32(self) -> i32 {
                self as i32
            }
        }
        impl AsI32 for u8 {
            #[inline]
            fn as_i32(self) -> i32 {
                self as i32
            }
        }
        impl AsI32 for char {
            #[inline]
            fn as_i32(self) -> i32 {
                self as i32
            }
        }
        impl AsI32 for usize {
            #[inline]
            fn as_i32(self) -> i32 {
                self as i32
            }
        }
        pub use alloc_crate::alloc;
        extern crate alloc as alloc_crate;
    }
    #[doc = " Generates `#[unsafe(no_mangle)]` functions to export the specified type as"]
    #[doc = " the root implementation of all generated traits."]
    #[doc = ""]
    #[doc = " For more information see the documentation of `wit_bindgen::generate!`."]
    #[doc = ""]
    #[doc = " ```rust"]
    #[doc = " # macro_rules! export{ ($($t:tt)*) => (); }"]
    #[doc = " # trait Guest {}"]
    #[doc = " struct MyType;"]
    #[doc = ""]
    #[doc = " impl Guest for MyType {"]
    #[doc = "     // ..."]
    #[doc = " }"]
    #[doc = ""]
    #[doc = " export!(MyType);"]
    #[doc = " ```"]
    #[allow(unused_macros)]
    #[doc(hidden)]
    macro_rules! __export_api_impl {
    ($ty:ident) => (self::export!($ty with_types_in self);
    );
    ($ty:ident with_types_in$($path_to_types_root:tt)*) => ($($path_to_types_root)*::exports::townframe::daybook_api::ctx::__export_townframe_daybook_api_ctx_cabi!($ty with_types_in$($path_to_types_root)*::exports::townframe::daybook_api::ctx);
    $($path_to_types_root)*::exports::townframe::daybook_api::doc_create::__export_townframe_daybook_api_doc_create_cabi!($ty with_types_in$($path_to_types_root)*::exports::townframe::daybook_api::doc_create);
    )
}
    #[doc(inline)]
    pub(crate) use __export_api_impl as export;
    #[cfg(target_arch = "wasm32")]
    #[unsafe(link_section = "component-type:wit-bindgen:0.48.1:townframe:daybook-api:api:encoded world")]
    #[doc(hidden)]
    #[allow(clippy::octal_escapes)]
    pub static __WIT_BINDGEN_COMPONENT_TYPE: [u8; 5691] = *b"\
\0asm\x0d\0\x01\0\0\x19\x16wit-component-encoding\x04\0\x07\xc1+\x01A\x02\x01A/\x01\
B\x0a\x04\0\x08pollable\x03\x01\x01h\0\x01@\x01\x04self\x01\0\x7f\x04\0\x16[meth\
od]pollable.ready\x01\x02\x01@\x01\x04self\x01\x01\0\x04\0\x16[method]pollable.b\
lock\x01\x03\x01p\x01\x01py\x01@\x01\x02in\x04\0\x05\x04\0\x04poll\x01\x06\x03\0\
\x12wasi:io/poll@0.2.6\x05\0\x02\x03\0\0\x08pollable\x01B\x0f\x02\x03\x02\x01\x01\
\x04\0\x08pollable\x03\0\0\x01w\x04\0\x07instant\x03\0\x02\x01w\x04\0\x08duratio\
n\x03\0\x04\x01@\0\0\x03\x04\0\x03now\x01\x06\x01@\0\0\x05\x04\0\x0aresolution\x01\
\x07\x01i\x01\x01@\x01\x04when\x03\0\x08\x04\0\x11subscribe-instant\x01\x09\x01@\
\x01\x04when\x05\0\x08\x04\0\x12subscribe-duration\x01\x0a\x03\0!wasi:clocks/mon\
otonic-clock@0.2.6\x05\x02\x01B\x05\x01r\x02\x07secondsw\x0bnanosecondsy\x04\0\x08\
datetime\x03\0\0\x01@\0\0\x01\x04\0\x03now\x01\x02\x04\0\x0aresolution\x01\x02\x03\
\0\x1cwasi:clocks/wall-clock@0.2.6\x05\x03\x01B\x0b\x01q\x02\x08upstream\x01s\0\x02\
io\x01s\0\x04\0\x0cconfig-error\x03\0\0\x01ks\x01j\x01\x02\x01\x01\x01@\x01\x03k\
eys\0\x03\x04\0\x03get\x01\x04\x01o\x02ss\x01p\x05\x01j\x01\x06\x01\x01\x01@\0\0\
\x07\x04\0\x07get-all\x01\x08\x03\0\x1fwasi:config/runtime@0.2.0-draft\x05\x04\x01\
B\x1c\x01q\x03\x0dno-such-store\0\0\x0daccess-denied\0\0\x05other\x01s\0\x04\0\x05\
error\x03\0\0\x01ps\x01kw\x01r\x02\x04keys\x02\x06cursor\x03\x04\0\x0ckey-respon\
se\x03\0\x04\x04\0\x06bucket\x03\x01\x01h\x06\x01p}\x01k\x08\x01j\x01\x09\x01\x01\
\x01@\x02\x04self\x07\x03keys\0\x0a\x04\0\x12[method]bucket.get\x01\x0b\x01j\0\x01\
\x01\x01@\x03\x04self\x07\x03keys\x05value\x08\0\x0c\x04\0\x12[method]bucket.set\
\x01\x0d\x01@\x02\x04self\x07\x03keys\0\x0c\x04\0\x15[method]bucket.delete\x01\x0e\
\x01j\x01\x7f\x01\x01\x01@\x02\x04self\x07\x03keys\0\x0f\x04\0\x15[method]bucket\
.exists\x01\x10\x01j\x01\x05\x01\x01\x01@\x02\x04self\x07\x06cursor\x03\0\x11\x04\
\0\x18[method]bucket.list-keys\x01\x12\x01i\x06\x01j\x01\x13\x01\x01\x01@\x01\x0a\
identifiers\0\x14\x04\0\x04open\x01\x15\x03\0\x1fwasi:keyvalue/store@0.2.0-draft\
\x05\x05\x02\x03\0\x04\x06bucket\x02\x03\0\x04\x05error\x01B\x08\x02\x03\x02\x01\
\x06\x04\0\x06bucket\x03\0\0\x02\x03\x02\x01\x07\x04\0\x05error\x03\0\x02\x01h\x01\
\x01j\x01w\x01\x03\x01@\x03\x06bucket\x04\x03keys\x05deltaw\0\x05\x04\0\x09incre\
ment\x01\x06\x03\0!wasi:keyvalue/atomics@0.2.0-draft\x05\x08\x01B\x04\x01m\x06\x05\
trace\x05debug\x04info\x04warn\x05error\x08critical\x04\0\x05level\x03\0\0\x01@\x03\
\x05level\x01\x07contexts\x07messages\x01\0\x04\0\x03log\x01\x02\x03\0\x20wasi:l\
ogging/logging@0.1.0-draft\x05\x09\x01Bl\x01q\x03\x0einvalid-params\x01s\0\x0din\
valid-query\x01s\0\x0aunexpected\x01s\0\x04\0\x0bquery-error\x03\0\0\x01q\x01\x0a\
unexpected\x01s\0\x04\0\x17statement-prepare-error\x03\0\x02\x01q\x03\x16unknown\
-prepared-query\0\0\x0bquery-error\x01\x01\0\x0aunexpected\x01s\0\x04\0\x1dprepa\
red-statement-exec-error\x03\0\x04\x01o\x03w|~\x04\0\x0chashable-f64\x03\0\x06\x04\
\0\x0chashable-f32\x03\0\x07\x01o\x02\x07\x07\x04\0\x05point\x03\0\x09\x04\0\x10\
lower-left-point\x03\0\x0a\x04\0\x11upper-right-point\x03\0\x0a\x04\0\x0bstart-p\
oint\x03\0\x0a\x04\0\x09end-point\x03\0\x0a\x04\0\x0ccenter-point\x03\0\x0a\x04\0\
\x06radius\x03\0\x07\x01s\x04\0\x09ipv4-addr\x03\0\x11\x01s\x04\0\x09ipv6-addr\x03\
\0\x13\x01s\x04\0\x06subnet\x03\0\x15\x01x\x04\0\x04xmin\x03\0\x17\x01x\x04\0\x04\
xmax\x03\0\x19\x01px\x04\0\x08xip-list\x03\0\x1b\x01y\x04\0\x0blogfile-num\x03\0\
\x1d\x01y\x04\0\x13logfile-byte-offset\x03\0\x1f\x01s\x04\0\x0bcolumn-name\x03\0\
!\x01s\x04\0\x07numeric\x03\0#\x01m\x04\x01A\x01B\x01C\x01D\x04\0\x0dlexeme-weig\
ht\x03\0%\x01k{\x01k&\x01r\x03\x08position'\x06weight(\x04datas\x04\0\x06lexeme\x03\
\0)\x01q\x02\x17eastern-hemisphere-secs\x01z\0\x17western-hemisphere-secs\x01z\0\
\x04\0\x06offset\x03\0+\x01o\x03zyy\x01q\x03\x11positive-infinity\0\0\x11negativ\
e-infinity\0\0\x03ymd\x01-\0\x04\0\x04date\x03\0.\x01r\x04\x05start/\x0fstart-in\
clusive\x7f\x03end/\x0dend-inclusive\x7f\x04\0\x08interval\x03\00\x01r\x04\x04ho\
ury\x03miny\x03secy\x05microy\x04\0\x04time\x03\02\x01r\x02\x09timesonzes\x04tim\
e3\x04\0\x07time-tz\x03\04\x01r\x02\x04date/\x04time3\x04\0\x09timestamp\x03\06\x01\
r\x02\x09timestamp7\x06offset,\x04\0\x0ctimestamp-tz\x03\08\x01o\x06}}}}}}\x01r\x01\
\x05bytes:\x04\0\x11mac-address-eui48\x03\0;\x01o\x08}}}}}}}}\x01r\x01\x05bytes=\
\x04\0\x11mac-address-eui64\x03\0>\x01px\x01p\x7f\x01p\x07\x01p\x08\x01pz\x01p$\x01\
p|\x01p\xc6\0\x01p}\x01o\x02y\xc8\0\x01p\xc9\0\x01ky\x01o\x02\xcb\0\xc8\0\x01p\xcc\
\0\x01p\xc8\0\x01ps\x01p<\x01p?\x01o\x02\x0b\x0c\x01p\xd2\0\x01o\x02\x0f\x10\x01\
p\xd4\0\x01o\x02\x0d\x0e\x01p\xd6\0\x01p\x0a\x01p\xd8\0\x01p/\x01p1\x01p3\x01p5\x01\
p7\x01p9\x01pw\x01o\x03\x18\x1a\x1c\x01p*\x01ks\x01o\x02s\xe3\0\x01p\xe4\0\x01qa\
\x04null\0\0\x07big-int\x01x\0\x04int8\x01x\0\x0aint8-array\x01\xc0\0\0\x0abig-s\
erial\x01x\0\x07serial8\x01x\0\x04bool\x01\x7f\0\x07boolean\x01\x7f\0\x0abool-ar\
ray\x01\xc1\0\0\x06double\x01\x07\0\x06float8\x01\x07\0\x0cfloat8-array\x01\xc2\0\
\0\x04real\x01\x08\0\x06float4\x01\x08\0\x0cfloat4-array\x01\xc3\0\0\x07integer\x01\
z\0\x03int\x01z\0\x04int4\x01z\0\x0aint4-array\x01\xc4\0\0\x07numeric\x01$\0\x07\
decimal\x01$\0\x0dnumeric-array\x01\xc5\0\0\x06serial\x01y\0\x07serial4\x01y\0\x09\
small-int\x01|\0\x04int2\x01|\0\x0aint2-array\x01\xc6\0\0\x0bint2-vector\x01\xc6\
\0\0\x11int2-vector-array\x01\xc7\0\0\x0csmall-serial\x01|\0\x07serial2\x01|\0\x03\
bit\x01\xc9\0\0\x09bit-array\x01\xca\0\0\x0bbit-varying\x01\xcc\0\0\x06varbit\x01\
\xcc\0\0\x0cvarbit-array\x01\xcd\0\0\x05bytea\x01\xc8\0\0\x0bbytea-array\x01\xce\
\0\0\x04char\x01\xc9\0\0\x0achar-array\x01\xca\0\0\x07varchar\x01\xcc\0\0\x0dvar\
char-array\x01\xcd\0\0\x04cidr\x01s\0\x0acidr-array\x01\xcf\0\0\x04inet\x01s\0\x0a\
inet-array\x01\xcf\0\0\x07macaddr\x01<\0\x0dmacaddr-array\x01\xd0\0\0\x08macaddr\
8\x01?\0\x0emacaddr8-array\x01\xd1\0\0\x03box\x01\xd2\0\0\x09box-array\x01\xd3\0\
\0\x06circle\x01\xd4\0\0\x0ccircle-array\x01\xd5\0\0\x04line\x01\xd6\0\0\x0aline\
-array\x01\xd7\0\0\x04lseg\x01\xd6\0\0\x0alseg-array\x01\xd7\0\0\x04path\x01\xd8\
\0\0\x0apath-array\x01\xd9\0\0\x05point\x01\x0a\0\x0bpoint-array\x01\xd8\0\0\x07\
polygon\x01\xd8\0\0\x0dpolygon-array\x01\xd9\0\0\x04date\x01/\0\x0adate-array\x01\
\xda\0\0\x08interval\x011\0\x0einterval-array\x01\xdb\0\0\x04time\x013\0\x0atime\
-array\x01\xdc\0\0\x07time-tz\x015\0\x0dtime-tz-array\x01\xdd\0\0\x09timestamp\x01\
7\0\x0ftimestamp-array\x01\xde\0\0\x0ctimestamp-tz\x019\0\x12timestamp-tz-array\x01\
\xdf\0\0\x04json\x01s\0\x0ajson-array\x01\xcf\0\0\x05jsonb\x01s\0\x0bjsonb-array\
\x01\xcf\0\0\x05money\x01$\0\x0bmoney-array\x01\xc5\0\0\x06pg-lsn\x01w\0\x0cpg-l\
sn-array\x01\xe0\0\0\x0bpg-snapshot\x01\xe1\0\0\x0dtxid-snapshot\x01x\0\x04name\x01\
s\0\x0aname-array\x01\xcf\0\0\x04text\x01s\0\x0atext-array\x01\xcf\0\0\x03xml\x01\
s\0\x09xml-array\x01\xcf\0\0\x08ts-query\x01s\0\x09ts-vector\x01\xe2\0\0\x04uuid\
\x01s\0\x0auuid-array\x01\xcf\0\0\x06hstore\x01\xe5\0\0\x04\0\x08pg-value\x03\0f\
\x01r\x02\x0bcolumn-names\x05value\xe7\0\x04\0\x10result-row-entry\x03\0h\x01p\xe9\
\0\x04\0\x0aresult-row\x03\0j\x03\0$wasmcloud:postgres/types@0.1.1-draft\x05\x0a\
\x02\x03\0\x07\x08pg-value\x02\x03\0\x07\x0aresult-row\x02\x03\0\x07\x0bquery-er\
ror\x01B\x0e\x02\x03\x02\x01\x0b\x04\0\x08pg-value\x03\0\0\x02\x03\x02\x01\x0c\x04\
\0\x0aresult-row\x03\0\x02\x02\x03\x02\x01\x0d\x04\0\x0bquery-error\x03\0\x04\x01\
p\x01\x01p\x03\x01j\x01\x07\x01\x05\x01@\x02\x05querys\x06params\x06\0\x08\x04\0\
\x05query\x01\x09\x01j\0\x01\x05\x01@\x01\x05querys\0\x0a\x04\0\x0bquery-batch\x01\
\x0b\x03\0$wasmcloud:postgres/query@0.1.1-draft\x05\x0e\x02\x03\0\x02\x08datetim\
e\x01B\x0f\x02\x03\x02\x01\x0f\x04\0\x02dt\x03\0\0\x01o\x02ss\x01p\x02\x01r\x01\x06\
issues\x03\x04\0\x11errors-validation\x03\0\x04\x01r\x01\x07messages\x04\0\x0eer\
ror-internal\x03\0\x06\x04\0\x08datetime\x03\0\x01\x01s\x04\0\x04uuid\x03\0\x09\x01\
o\x04\x05\x07\x08\x0a\x01j\0\0\x01@\x01\x03inc\x0b\0\x0c\x04\0\x04noop\x01\x0d\x03\
\0\x19townframe:api-utils/utils\x05\x10\x02\x03\0\x09\x11errors-validation\x02\x03\
\0\x09\x0eerror-internal\x02\x03\0\x09\x04uuid\x02\x03\0\x09\x08datetime\x01B\x1e\
\x02\x03\x02\x01\x11\x04\0\x11errors-validation\x03\0\0\x02\x03\x02\x01\x12\x04\0\
\x0eerror-internal\x03\0\x02\x02\x03\x02\x01\x13\x04\0\x04uuid\x03\0\x04\x02\x03\
\x02\x01\x14\x04\0\x08datetime\x03\0\x06\x01s\x04\0\x09mime-type\x03\0\x08\x01s\x04\
\0\x06doc-id\x03\0\x0a\x01r\x02\x0dlength-octetsw\x04hash\x0b\x04\0\x08doc-blob\x03\
\0\x0c\x01s\x04\0\x09mutlihash\x03\0\x0e\x01k\x0f\x01r\x05\x04mime\x09\x08width-\
pxw\x09height-pxw\x08blurhash\x10\x04blob\x0f\x04\0\x09doc-image\x03\0\x11\x01m\x03\
\x04text\x04blob\x05image\x04\0\x08doc-kind\x03\0\x13\x01q\x03\x04text\x01s\0\x04\
blob\x01\x0d\0\x05image\x01\x12\0\x04\0\x0bdoc-content\x03\0\x15\x01m\x02\x0bref\
-generic\x0dlabel-generic\x04\0\x0cdoc-tag-kind\x03\0\x17\x01q\x02\x0bref-generi\
c\x01\x0f\0\x0dlabel-generic\x01s\0\x04\0\x07doc-tag\x03\0\x19\x01p\x1a\x01r\x05\
\x02id\x0f\x0acreated-at\x07\x0aupdated-at\x07\x07content\x14\x04tags\x1b\x04\0\x03\
doc\x03\0\x1c\x03\0\x19townframe:daybook-api/doc\x05\x15\x01B\x03\x01j\0\x01s\x01\
@\0\0\0\x04\0\x04init\x01\x01\x04\0\x19townframe:daybook-api/ctx\x05\x16\x02\x03\
\0\x0a\x09mime-type\x02\x03\0\x0a\x06doc-id\x02\x03\0\x0a\x09doc-image\x02\x03\0\
\x0a\x08doc-blob\x02\x03\0\x0a\x09mutlihash\x02\x03\0\x0a\x08doc-kind\x02\x03\0\x0a\
\x0bdoc-content\x02\x03\0\x0a\x0cdoc-tag-kind\x02\x03\0\x0a\x07doc-tag\x02\x03\0\
\x0a\x03doc\x01B+\x02\x03\x02\x01\x11\x04\0\x11errors-validation\x03\0\0\x02\x03\
\x02\x01\x12\x04\0\x0eerror-internal\x03\0\x02\x02\x03\x02\x01\x13\x04\0\x04uuid\
\x03\0\x04\x02\x03\x02\x01\x14\x04\0\x08datetime\x03\0\x06\x02\x03\x02\x01\x17\x04\
\0\x09mime-type\x03\0\x08\x02\x03\x02\x01\x18\x04\0\x06doc-id\x03\0\x0a\x02\x03\x02\
\x01\x19\x04\0\x09doc-image\x03\0\x0c\x02\x03\x02\x01\x1a\x04\0\x08doc-blob\x03\0\
\x0e\x02\x03\x02\x01\x1b\x04\0\x09mutlihash\x03\0\x10\x02\x03\x02\x01\x1c\x04\0\x08\
doc-kind\x03\0\x12\x02\x03\x02\x01\x1d\x04\0\x0bdoc-content\x03\0\x14\x02\x03\x02\
\x01\x1e\x04\0\x0cdoc-tag-kind\x03\0\x16\x02\x03\x02\x01\x1f\x04\0\x07doc-tag\x03\
\0\x18\x02\x03\x02\x01\x20\x04\0\x03doc\x03\0\x1a\x01r\x01\x02ids\x04\0\x11error\
-id-occupied\x03\0\x1c\x01q\x03\x0bid-occupied\x01\x1d\0\x0dinvalid-input\x01\x01\
\0\x08internal\x01\x03\0\x04\0\x05error\x03\0\x1e\x01r\x01\x02id\x05\x04\0\x05in\
put\x03\0\x20\x04\0\x06output\x03\0\x1b\x04\0\x07service\x03\x01\x01i#\x01@\0\0$\
\x04\0\x14[constructor]service\x01%\x01h#\x01j\x01\"\x01\x1f\x01@\x02\x04self&\x03\
inp!\0'\x04\0\x15[method]service.serve\x01(\x04\0\x20townframe:daybook-api/doc-c\
reate\x05!\x04\0\x19townframe:daybook-api/api\x04\0\x0b\x09\x01\0\x03api\x03\0\0\
\0G\x09producers\x01\x0cprocessed-by\x02\x0dwit-component\x070.241.2\x10wit-bind\
gen-rust\x060.48.1";
    #[inline(never)]
    #[doc(hidden)]
    pub fn __link_custom_section_describing_imports() {
        wit_bindgen::rt::maybe_link_cabi_realloc();
    }
    const _: &[u8] =
        include_bytes!(r#"/home/asdf/repos/rust/townframe/src/daybook_api/wit/package.wit"#);
    const _: &[u8] =
        include_bytes!(r#"/home/asdf/repos/rust/townframe/src/daybook_api/wit/doc.wit"#);
    const _: &[u8] = include_bytes!(
        r#"/home/asdf/repos/rust/townframe/src/daybook_api/wit/deps/api-utils/package.wit"#
    );
    const _: &[u8] = include_bytes!(
        r#"/home/asdf/repos/rust/townframe/src/daybook_api/wit/deps/api-utils/imports.wit"#
    );
    const _: &[u8] = include_bytes!(
        r#"/home/asdf/repos/rust/townframe/src/daybook_api/wit/deps/wasi-clocks-0.2.6/package.wit"#
    );
    const _: &[u8] = include_bytes!(
        r#"/home/asdf/repos/rust/townframe/src/daybook_api/wit/deps/wasi-config-0.2.0-draft/package.wit"#
    );
    const _: &[u8] = include_bytes!(
        r#"/home/asdf/repos/rust/townframe/src/daybook_api/wit/deps/wasi-io-0.2.6/package.wit"#
    );
    const _: &[u8] = include_bytes!(
        r#"/home/asdf/repos/rust/townframe/src/daybook_api/wit/deps/wasi-keyvalue-0.2.0-draft/package.wit"#
    );
    const _: &[u8] = include_bytes!(
        r#"/home/asdf/repos/rust/townframe/src/daybook_api/wit/deps/wasi-logging-0.1.0-draft/package.wit"#
    );
    const _: &[u8] = include_bytes!(
        r#"/home/asdf/repos/rust/townframe/src/daybook_api/wit/deps/wasmcloud-postgres-0.1.1-draft/package.wit"#
    );
}

wit::export!(Component with_types_in wit);

struct Component;

impl wit::exports::townframe::daybook_api::ctx::Guest for Component {
    fn init() -> Result<(), String> {
        crate::init().map_err(|err| format!("{err:?}"))?;
        Ok(())
    }
}
impl wit::exports::townframe::daybook_api::doc_create::Guest for Component {
    type Service = doc::create::DocCreate;
}
