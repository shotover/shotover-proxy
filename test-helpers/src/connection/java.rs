use anyhow::Result;
use j4rs::{Instance, InvocationArg, Jvm, JvmBuilder, MavenArtifact};
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

#[derive(Clone)]
pub(crate) struct Java(Rc<Jvm>);

impl Java {
    pub(crate) fn new(deps: &[&str]) -> Self {
        let jvm = Rc::new(JvmBuilder::new().build().unwrap());

        for dep in deps {
            jvm.deploy_artifact(&MavenArtifact::from(*dep)).unwrap();
        }
        Self(jvm)
    }

    /// Call the java constructor with the provided name and args
    pub(crate) fn construct(&self, name: &str, args: Vec<Value>) -> Value {
        self.construct_fallible(name, args).unwrap()
    }

    /// Call the java constructor with the provided name and args
    pub(crate) fn construct_fallible(&self, name: &str, args: Vec<Value>) -> Result<Value> {
        let args: Vec<InvocationArg> = args.into_iter().map(|x| x.instance.into()).collect();
        Ok(Value {
            instance: self.0.create_instance(name, &args)?,
            jvm: self.0.clone(),
        })
    }

    pub(crate) fn new_string(&self, string: &str) -> Value {
        Value {
            instance: self
                .0
                .create_instance(
                    "java.lang.String",
                    &[&InvocationArg::try_from(string).unwrap()],
                )
                .unwrap(),
            jvm: self.0.clone(),
        }
    }

    fn new_primitive(&self, class_name: &str, convert_method: &str, value: InvocationArg) -> Value {
        Value {
            instance: self
                .0
                .invoke(
                    &self
                        .0
                        .create_instance(
                            // e.g. "java.lang.Long",
                            class_name,
                            &[&value.into_primitive().unwrap()],
                        )
                        .unwrap(),
                    // e.g. "longValue"
                    convert_method,
                    InvocationArg::empty(),
                )
                .unwrap(),
            jvm: self.0.clone(),
        }
    }

    pub(crate) fn new_long(&self, value: i64) -> Value {
        self.new_primitive(
            "java.lang.Long",
            "longValue",
            InvocationArg::try_from(value).unwrap(),
        )
    }

    pub(crate) fn new_int(&self, value: i32) -> Value {
        self.new_primitive(
            "java.lang.Integer",
            "intValue",
            InvocationArg::try_from(value).unwrap(),
        )
    }

    pub(crate) fn new_short(&self, value: i16) -> Value {
        self.new_primitive(
            "java.lang.Short",
            "shortValue",
            InvocationArg::try_from(value).unwrap(),
        )
    }

    pub(crate) fn new_list(&self, inner_class_name: &str, args: &[InvocationArg]) -> Value {
        // TODO: Discuss with upstream why this was deprecated, `Jvm::java_list` is very difficult to use due to Result in input.
        Value {
            #[allow(deprecated)]
            instance: self.0.create_java_list(inner_class_name, args).unwrap(),
            jvm: self.0.clone(),
        }
    }

    pub(crate) fn new_map(&self, key_values: Vec<(Value, Value)>) -> Value {
        let map = self.construct("java.util.HashMap", vec![]);
        for (k, v) in key_values {
            map.call("put", &[&k.into(), &v.into()]);
        }
        map
    }

    pub(crate) fn invoke_static(
        &self,
        class_name: &str,
        method_name: &str,
        args: &[&InvocationArg],
    ) -> Value {
        self.invoke_static_fallible(class_name, method_name, args)
            .unwrap()
    }

    pub(crate) fn invoke_static_fallible(
        &self,
        class_name: &str,
        method_name: &str,
        args: &[&InvocationArg],
    ) -> Result<Value> {
        Ok(Value {
            instance: self.0.invoke_static(class_name, method_name, args)?,
            jvm: self.0.clone(),
        })
    }

    pub(crate) fn static_class(&self, class_name: &str) -> Value {
        self.static_class_fallible(class_name).unwrap()
    }

    fn static_class_fallible(&self, class_name: &str) -> Result<Value> {
        Ok(Value {
            instance: self.0.static_class(class_name)?,
            jvm: self.0.clone(),
        })
    }
}

pub(crate) struct Value {
    pub instance: Instance,
    jvm: Rc<Jvm>,
}

impl Value {
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

    pub(crate) fn field(&self, field_name: &str) -> Self {
        self.field_fallible(field_name).unwrap()
    }

    fn field_fallible(&self, field_name: &str) -> Result<Self> {
        let instance = self.jvm.field(&self.instance, field_name)?;
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
