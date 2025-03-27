use std::{net::SocketAddr, time::Duration};

use futures::{stream::FuturesUnordered, StreamExt, TryStreamExt};
#[cfg(feature = "serde")]
use nomos_utils::bounded_duration::{MinimalBoundedDuration, NANO};
use sntpc::{get_time, Error as SntpError, NtpContext, NtpResult, StdTimestampGen};
use tokio::{
    net::{lookup_host, ToSocketAddrs, UdpSocket},
    time::{error::Elapsed, timeout},
};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Io(std::io::Error),
    #[error("NTP internal error: {0:?}")]
    Sntp(SntpError),
    #[error("NTP request timeout, elapsed: {0:?}")]
    Timeout(Elapsed),
}

#[cfg_attr(feature = "serde", cfg_eval::cfg_eval, serde_with::serde_as)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Copy, Clone)]
pub struct NTPClientSettings {
    /// NTP server requests timeout duration
    #[cfg_attr(feature = "serde", serde_as(as = "MinimalBoundedDuration<1, NANO>"))]
    pub timeout: Duration,
    /// Local socket address to listen for responses
    pub local_socket: SocketAddr,
}

#[derive(Clone)]
pub struct AsyncNTPClient {
    settings: NTPClientSettings,
    ntp_context: NtpContext<StdTimestampGen>,
}

impl AsyncNTPClient {
    pub fn new(settings: NTPClientSettings) -> Self {
        let ntp_context = NtpContext::new(StdTimestampGen::default());
        Self {
            settings,
            ntp_context,
        }
    }

    /// Request a timestamp from an NTP server
    pub async fn request_timestamp<Addresses: ToSocketAddrs + Sync>(
        &self,
        pool: Addresses,
    ) -> Result<NtpResult, Error> {
        let socket = &UdpSocket::bind(self.settings.local_socket)
            .await
            .map_err(Error::Io)?;
        let hosts = lookup_host(&pool).await.map_err(Error::Io)?;
        let mut checks = hosts
            .map(move |host| get_time(host, socket, self.ntp_context))
            .collect::<FuturesUnordered<_>>()
            .into_stream();
        timeout(self.settings.timeout, checks.select_next_some())
            .await
            .map_err(Error::Timeout)?
            .map_err(Error::Sntp)
    }
}
