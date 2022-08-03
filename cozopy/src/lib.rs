// #[cfg(not(target_env = "msvc"))]
// use tikv_jemallocator::Jemalloc;
//
// #[cfg(not(target_env = "msvc"))]
// #[global_allocator]
// static GLOBAL: Jemalloc = Jemalloc;
//
use pyo3::exceptions::PyException;
use pyo3::prelude::*;

use cozo::{Db, DbBuilder};

#[pyclass(extends = PyException)]
struct ErrorBridge(cozo::Error);

trait PyResultExt<T> {
    fn into_py_res(self) -> PyResult<T>;
}

impl<T> PyResultExt<T> for anyhow::Result<T> {
    fn into_py_res(self) -> PyResult<T> {
        match self {
            Ok(t) => Ok(t),
            Err(e) => Err(PyException::new_err(e.to_string())),
        }
    }
}

#[pyclass]
struct CozoDbPy {
    db: Db,
}

#[pymethods]
impl CozoDbPy {
    #[new]
    #[args(create_if_missing = true, destroy_on_exit = false)]
    fn new(path: &str, create_if_missing: bool, destroy_on_exit: bool) -> PyResult<Self> {
        let _ = env_logger::try_init();
        let builder = DbBuilder::default()
            .path(path)
            .create_if_missing(create_if_missing)
            .destroy_on_exit(destroy_on_exit);
        let db = Db::build(builder).into_py_res()?;
        Ok(Self { db })
    }
    pub fn transact_attributes(&self, py: Python<'_>, payload: &str) -> PyResult<String> {
        let payload: serde_json::Value = serde_json::from_str(payload).unwrap();
        let ret = py.allow_threads(|| self.db.transact_attributes(&payload).into_py_res())?;
        Ok(ret.to_string())
    }
    pub fn transact_triples(&self, py: Python<'_>, payload: &str) -> PyResult<String> {
        let payload: serde_json::Value = serde_json::from_str(payload).unwrap();
        let ret = py.allow_threads(|| self.db.transact_triples(&payload).into_py_res())?;
        Ok(ret.to_string())
    }
    pub fn run_query(&self, py: Python<'_>, payload: &str) -> PyResult<String> {
        let payload: serde_json::Value = serde_json::from_str(payload).unwrap();
        let ret = py.allow_threads(|| self.db.run_query(&payload).into_py_res())?;
        Ok(ret.to_string())
    }
}

#[pymodule]
fn cozopy(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<CozoDbPy>()?;
    m.add_class::<ErrorBridge>()?;
    Ok(())
}
