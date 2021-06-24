use std::io::{BufReader, Read};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

type Error = Box<dyn std::error::Error + Send + Sync>;

pub const MAXIMUM_MESSAGE_SIZE: usize = 101 * 1024 * 1024; // 101 Mb

pub trait MessageName: Sized {
    fn from_str(input: &str) -> Result<Self, String>;
    fn as_bytes(&self) -> &'static [u8];
}

pub struct Message<T: MessageName> {
    pub name: T,
    pub data: Vec<u8>,
}

/// Message format:
/// 1 byte name length | name | 4 bytes data length | data
impl<T: MessageName> Message<T> {
    /// Read message name as Vec of bytes
    pub async fn read_name<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Vec<u8>, Error> {
        let mut name_length_buffer = [0u8; 1];
        reader.read_exact(&mut name_length_buffer).await?;
        let name_length = u8::from_be_bytes(name_length_buffer);
        let mut name = vec![0; name_length as usize];
        reader.read_exact(&mut name).await?;
        Ok(name)
    }

    /// Read message data as Vec of bytes
    pub async fn read_data<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Vec<u8>, Error> {
        let mut data_length_buffer = [0u8; 4];
        reader.read_exact(&mut data_length_buffer).await?;
        let data_length = u32::from_be_bytes(data_length_buffer) as usize;
        if data_length > MAXIMUM_MESSAGE_SIZE {
            return Err(format!(
                "Message length exceeds the limit: {} vs {}",
                data_length, MAXIMUM_MESSAGE_SIZE,
            )
            .into());
        }
        let mut data = vec![0; data_length];
        reader.read_exact(&mut data).await?;
        Ok(data)
    }

    /// Write entire message to a provided writer
    pub async fn write<W: AsyncWrite + Unpin>(&self, writer: &mut W) -> Result<(), Error> {
        self.write_name(writer).await?;
        self.write_data(writer).await?;
        Ok(())
    }

    /// Write only message name to a provided writer
    pub async fn write_name<W: AsyncWrite + Unpin>(&self, writer: &mut W) -> Result<(), Error> {
        let name = self.name.as_bytes();
        let name_length = (name.len() as u8).to_be_bytes();
        writer.write(&name_length).await?;
        writer.write(name).await?;
        Ok(())
    }

    /// Write only message name to a provided writer
    pub async fn write_data<W: AsyncWrite + Unpin>(&self, writer: &mut W) -> Result<(), Error> {
        let data_length = (self.data.len() as u32).to_be_bytes();
        writer.write(&data_length).await?;
        writer.write(&self.data).await?;
        Ok(())
    }

    /// Encodes self as a vector of bytes
    pub fn to_vec(&self) -> Vec<u8> {
        let name = self.name.as_bytes();
        let name_length = (name.len() as u8).to_be_bytes();
        let data_length = (self.data.len() as u32).to_be_bytes();
        [&name_length, name, &data_length, &self.data].concat()
    }

    /// Decodes Self from a slice of bytes
    pub fn from_slice(bytes: &[u8]) -> Result<Self, Error> {
        let mut reader = BufReader::new(bytes);

        let mut name_length_buffer = [0u8; 1];
        reader.read_exact(&mut name_length_buffer)?;
        let name_length = u8::from_be_bytes(name_length_buffer);
        let mut name = vec![0; name_length as usize];
        reader.read_exact(&mut name)?;

        let mut data_length_buffer = [0u8; 4];
        reader.read_exact(&mut data_length_buffer)?;
        let data_length = u32::from_be_bytes(data_length_buffer) as usize;
        if data_length > MAXIMUM_MESSAGE_SIZE {
            return Err(format!(
                "Message length exceeds the limit: {} vs {}",
                data_length, MAXIMUM_MESSAGE_SIZE,
            )
            .into());
        }
        let mut data = vec![0; data_length as usize];
        reader.read_exact(&mut data)?;

        let name_string = String::from_utf8(name)?;

        Ok(Self {
            name: T::from_str(&name_string)?,
            data,
        })
    }
}

#[cfg(test)]
use super::ContributorMessageName;

#[test]
fn message_round_trip() {
    let message = Message {
        name: ContributorMessageName::CpuChallenge,
        data: vec![1, 2, 3],
    };
    let encoded = message.to_vec();
    let decoded = Message::<ContributorMessageName>::from_slice(&encoded).unwrap();
    assert_eq!(decoded.name, message.name);
    assert_eq!(decoded.data, message.data);
}
