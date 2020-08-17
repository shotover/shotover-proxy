# Functions
Functions in shotover can be defined or used where ever a transform expects a ScriptConfigurator
object. A ScriptConfigurator provides shotover with enough information to call and run your scripts.
Currently shotover supports two ways of running user defined code.

The first is to run Lua script in a lua VM, the second is to run WASM in a wasm sandbox. 

You can pass any Lua code into any ScriptConfigurator or provide the path to any wasm binary as long as it
uses the WASI ABI. From there you will also need to define the script type (`lua` or `wasm`) and the name of the
function to be called.

The Transform or other area where you will provide a script, will provide documentation around the expected
function signature as well as any behavior to expect.

## Lua
_State: Alpha_

Within the global user environment you have access to the following:

* CoRoutine std lib
* Table std lib
* IO std lib
* OS std lib
* String std lib
* Bit std lib
* Math std lib
* Package std lib

When configuring a Lua function, whether its for a function in a Transform, or the Lua transform itself, you may have
additional access to other specific functions depending on the context of the lua script (e.g. defined by the transform
it's attached to).

* `script_type` - This is a string that is either the value `lua` or `wasm`
* `function_name` - This is the function name to call. Your definition may have multiple functions. In
Lua you are able to also just leave this blank and it will just execute the code fragment. Parameters will
be passed in as globals.
* `script_definition` - A string with your Lua code in it.

## WASM 
_State: Alpha_

* `script_type` - This is a string that is either the value `lua` or `wasm`
* `function_name` - This is the function name to call.
* `script_definition` - A string with the full path that points to your wasm assembly code.