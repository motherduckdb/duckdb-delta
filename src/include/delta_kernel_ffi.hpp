#pragma once

// NOTE:
// - delta_kernel_ffi.hpp is part of the kernel build, and won't exist until the cargo build step runs.
// - DEBUG is a defined enum in the ffi code. This is a problem for us in C land. undef'ing hackery
//   and in the log-translation code is enough.
// - clang complains a lot about the rust FFI symbols and c-linkage; the clang pragmas avoid this.

#pragma push_macro("DEBUG")
#undef DEBUG

#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wreturn-type-c-linkage"
#endif //__clang__

#include "ffi-headers/delta_kernel_ffi.hpp"

#if defined(__clang__)
#pragma clang diagnostic pop
#endif //__clang__

#pragma pop_macro("DEBUG")

namespace ffi {

extern "C" {
// This trickery is from https://github.com/mozilla/cbindgen/issues/402#issuecomment-578680163
struct im_an_unused_struct_that_tricks_msvc_into_compilation {
	ExternResult<KernelBoolSlice> field;
	ExternResult<bool> field2;
	ExternResult<EngineBuilder *> field3;
	ExternResult<Handle<SharedExternEngine>> field4;
	ExternResult<Handle<SharedSnapshot>> field5;
	ExternResult<uintptr_t> field6;
	ExternResult<ArrowFFIData *> field7;
	ExternResult<Handle<SharedScanMetadataIterator>> field8;
	ExternResult<Handle<SharedScan>> field9;
	ExternResult<Handle<ExclusiveFileReadResultIterator>> field10;
	ExternResult<KernelRowIndexArray> field11;
	ExternResult<Handle<ExclusiveEngineData>> field12;
	ExternResult<Handle<ExclusiveTransaction>> field13;
	ExternResult<uint64_t> field14;
	ExternResult<NullableCvoid> field15;
	ExternResult<Handle<SharedExpressionEvaluator>> field16;
	ExternResult<Handle<ExclusiveTableChanges>> field17;
	ExternResult<Handle<SharedTableChangesScan>> field18;
	ExternResult<Handle<SharedScanTableChangesIterator>> field19;
	ExternResult<ArrowFFIData> field20;
	ExternResult<OptionalValue<int64_t>> field21;
	OptionalValue<Handle<SharedExpression>> field22;
	ExternResult<Handle<SharedExpression>> field23;
	ExternResult<Handle<SharedPredicate>> field24;
};

} // extern "C"

} // namespace ffi
