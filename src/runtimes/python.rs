use pyo3::prelude::*;
use pyo3::types::{IntoPyDict, PyDict};

pub struct PythonEnvironment {}

impl PythonEnvironment {
    pub fn eval_script(variables: &PyDict, script: &str, py: Python) -> Result<(), PyErr> {
        let globals = [
            ("os", py.import("os")?),
            ("sys", py.import("sys")?),
            ("__builtins__", py.import("builtins")?),
        ]
        .into_py_dict(py);
        for (k, v) in variables {
            globals.set_item(k, v)?;
        }
        py.run(script, Some(globals), None).map_err(|e| {
            emit_compile_error_msg(py, &e, script);
            return e;
        })?;
        Ok(())
    }
}

//TODO instead of panicing, log the error.
fn emit_compile_error_msg(py: Python, error: &PyErr, script: &str) {
    use pyo3::type_object::PyTypeObject;
    use pyo3::AsPyRef;

    let value = error.to_object(py);

    if value.is_none() {
        panic!(format!("python: {}", error.ptype.as_ref(py).name()));
    }

    if error.matches(py, pyo3::exceptions::SyntaxError::type_object()) {
        let line: Option<usize> = value
            .getattr(py, "lineno")
            .ok()
            .and_then(|x| x.extract(py).ok());
        let msg: Option<String> = value
            .getattr(py, "msg")
            .ok()
            .and_then(|x| x.extract(py).ok());
        let text: Option<String> = value
            .getattr(py, "text")
            .ok()
            .and_then(|x| x.extract(py).ok());
        let offset: Option<usize> = value
            .getattr(py, "offset")
            .ok()
            .and_then(|x| x.extract(py).ok());
        if let (Some(line), Some(msg), Some(text), Some(offset)) = (line, msg, text, offset) {
            panic!(format!(
                "\nerror {} - line {}\n{}\n{}^",
                line,
                msg,
                text,
                (0..offset).into_iter().map(|_| " ").collect::<String>()
            ));
        }
    }
    panic!(format!("python: {}", value.as_ref(py).str().unwrap()));
}
