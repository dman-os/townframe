// Opt-in child-process diagnostics for pinned Quint's Apalache RPC only.
// Requests/IR are never logged; preserve the original callback and exit status.
const Module = require('node:module');
const originalLoad = Module._load;
let installed = false;
Module._load = function (request, parent, isMain) {
  const loaded = originalLoad.apply(this, arguments);
  if (request === '@grpc/grpc-js' && !installed) {
    installed = true;
    const original = loaded.Client.prototype.makeUnaryRequest;
    loaded.Client.prototype.makeUnaryRequest = function (...args) {
      const index = args.length - 1;
      const callback = args[index];
      if (typeof callback !== 'function') throw new Error('Unexpected gRPC callback signature');
      args[index] = function (error, response) {
        if (error) console.error('quint-rpc-diagnostic:', JSON.stringify({
          code: error.code, details: String(error.details ?? '').slice(0, 4096),
          message: String(error.message ?? '').slice(0, 4096)
        }));
        if (response?.failure) console.error('quint-rpc-failure:', JSON.stringify({
          errorType: response.failure.errorType,
          data: String(response.failure.data ?? '').slice(0, 4096)
        }));
        return callback.apply(this, arguments);
      };
      return original.apply(this, args);
    };
  }
  if (request === './compileToTlaplus' && typeof loaded.compileToTlaplus === 'function') {
    const original = loaded.compileToTlaplus;
    loaded.compileToTlaplus = async function (...args) {
      const result = await original.apply(this,args);
      if (result.isLeft()) console.error('quint-translation-failure:', JSON.stringify({
        explanation: String(result.value.explanation ?? '').slice(0,4096),
        errors: JSON.stringify(result.value.errors ?? []).slice(0,4096),
        failure: JSON.stringify(result.value).slice(0,4096)
      }));
      return result;
    };
  }
  return loaded;
};
