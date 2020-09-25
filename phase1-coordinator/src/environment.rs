use crate::storage::InMemory;

use rocket_cors::{AllowedHeaders, AllowedOrigins, Cors};

pub type StorageType = InMemory;

#[derive(Debug, Copy, Clone)]
pub enum Environment {
    Test,
    Development,
    Production,
}

impl Environment {
    pub const fn version(&self) -> u64 {
        match self {
            Environment::Test => 1,
            Environment::Development => 1,
            Environment::Production => 1,
        }
    }

    pub const fn number_of_chunks(&self) -> u64 {
        match self {
            Environment::Test => 10,
            Environment::Development => 10,
            Environment::Production => 10,
        }
    }

    pub const fn address(&self) -> &str {
        match self {
            Environment::Test => "localhost",
            Environment::Development => "0.0.0.0",
            Environment::Production => "167.71.156.62",
        }
    }

    pub const fn port(&self) -> u16 {
        match self {
            Environment::Test => 8080,
            Environment::Development => 8080,
            Environment::Production => 8080,
        }
    }

    pub fn cors(&self) -> Cors {
        let allowed_origins = match self {
            Environment::Test => AllowedOrigins::all(),
            Environment::Development => AllowedOrigins::all(),
            Environment::Production => AllowedOrigins::all(),
        };

        let allowed_headers = match self {
            Environment::Test => AllowedHeaders::all(),
            Environment::Development => AllowedHeaders::all(),
            Environment::Production => AllowedHeaders::all(),
        };

        Cors {
            allowed_origins,
            allowed_headers,
            allow_credentials: true,
            ..Default::default()
        }
    }

    pub fn base_url(&self) -> String {
        format!("http://{}:{}", self.address(), self.port())
    }
}
