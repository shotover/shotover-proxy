use anyhow::Result;
use j4rs::{Instance, InvocationArg, Jvm};
use serde::de::DeserializeOwned;
use std::{any::Any, rc::Rc};

pub(crate) struct JavaIterator(Value);

impl JavaIterator {
    fn new(iterator_instance: Value) -> Self {
        JavaIterator(iterator_instance)
    }
}

impl Iterator for JavaIterator {
    type Item = Value;

    fn next(&mut self) -> Option<Value> {
        let has_next: bool = self.0.call("hasNext", &[]).into_rust();
        if has_next {
            Some(self.0.call("next", &[]))
        } else {
            None
        }
    }
}

struct Java(Rc<Jvm>);

impl Java {}

// enum ValueTy {
//     Arg(InvocationArg),
//     Instance(Instance),
// }

pub(crate) struct Value {
    pub instance: Instance,
    jvm: Rc<Jvm>,
}

impl Value {
    pub(crate) fn new_int(jvm: &Rc<Jvm>, int: i32) -> Self {
        Self {
            instance: InvocationArg::try_from(int).unwrap().instance().unwrap(),
            jvm: jvm.clone(),
        }
    }

    pub(crate) fn new_long(jvm: &Rc<Jvm>, int: i64) -> Self {
        Self {
            instance: Self::new(
                jvm,
                "java.lang.Long",
                &[&InvocationArg::try_from(int)
                    .unwrap()
                    .into_primitive()
                    .unwrap()],
            )
            .call("longValue", &[])
            .instance,
            jvm: jvm.clone(),
        }
    }

    pub(crate) fn new(jvm: &Rc<Jvm>, name: &str, args: &[&InvocationArg]) -> Self {
        Self::new_fallible(jvm, name, args).unwrap()
    }

    pub(crate) fn new_fallible(jvm: &Rc<Jvm>, name: &str, args: &[&InvocationArg]) -> Result<Self> {
        Ok(Self {
            instance: jvm.create_instance(name, args)?,
            jvm: jvm.clone(),
        })
    }

    pub(crate) fn new_list(jvm: &Rc<Jvm>, inner_class_name: &str, args: &[InvocationArg]) -> Self {
        // TODO: Discuss with upstream why this was deprecated, `Jvm::java_list` is very difficult to use due to Result in input.
        Self {
            #[allow(deprecated)]
            instance: jvm.create_java_list(inner_class_name, args).unwrap(),
            jvm: jvm.clone(),
        }
    }

    pub(crate) fn new_map(jvm: &Rc<Jvm>, key_values: Vec<(Value, Value)>) -> Value {
        let map = Value::new(jvm, "java.util.HashMap", &[]);
        for (k, v) in key_values {
            map.call("put", &[&k.into(), &v.into()]);
        }
        map
    }

    pub(crate) fn invoke_static(
        jvm: &Rc<Jvm>,
        class_name: &str,
        method_name: &str,
        args: &[&InvocationArg],
    ) -> Self {
        Self::invoke_static_fallible(jvm, class_name, method_name, args).unwrap()
    }

    pub(crate) fn invoke_static_fallible(
        jvm: &Rc<Jvm>,
        class_name: &str,
        method_name: &str,
        args: &[&InvocationArg],
    ) -> Result<Self> {
        Ok(Self {
            instance: jvm.invoke_static(class_name, method_name, args)?,
            jvm: jvm.clone(),
        })
    }

    pub(crate) fn call(&self, name: &str, args: &[&InvocationArg]) -> Self {
        self.call_fallible(name, args).unwrap()
    }

    fn call_fallible(&self, name: &str, args: &[&InvocationArg]) -> Result<Self> {
        let instance = self.jvm.invoke(&self.instance, name, args)?;
        Ok(Self {
            instance,
            jvm: self.jvm.clone(),
        })
    }

    pub(crate) async fn call_async(&self, name: &str, args: &[InvocationArg]) -> Self {
        self.call_async_fallible(name, args).await.unwrap()
    }

    pub(crate) async fn call_async_fallible(
        &self,
        name: &str,
        args: &[InvocationArg],
    ) -> Result<Self> {
        let instance = self.jvm.invoke_async(&self.instance, name, args).await?;
        Ok(Self {
            instance,
            jvm: self.jvm.clone(),
        })
    }

    pub(crate) fn cast(&self, name: &str) -> Self {
        self.cast_fallible(name).unwrap()
    }

    fn cast_fallible(&self, name: &str) -> Result<Self> {
        let instance = self.jvm.cast(&self.instance, name)?;
        Ok(Self {
            instance,
            jvm: self.jvm.clone(),
        })
    }

    pub(crate) fn into_rust<T>(self) -> T
    where
        T: DeserializeOwned + Any,
    {
        self.into_rust_fallible().unwrap()
    }

    fn into_rust_fallible<T>(self) -> Result<T>
    where
        T: DeserializeOwned + Any,
    {
        Ok(self.jvm.to_rust(self.instance)?)
    }
}

impl IntoIterator for Value {
    type Item = Value;

    type IntoIter = JavaIterator;

    fn into_iter(self) -> Self::IntoIter {
        JavaIterator::new(self)
    }
}

impl From<Value> for InvocationArg {
    fn from(value: Value) -> InvocationArg {
        value.instance.into()
    }
}
