pub(crate) const DEFAULT_KAFKA_HOST: &str = "localhost";
pub(crate) const DEFAULT_KAFKA_PORT: u16 = 9092;

pub(crate) fn parse_host_port(addr: &str) -> Option<(String, u16)> {
    let (host, port) = addr.rsplit_once(':')?;
    let port = port.parse::<u16>().ok()?;
    Some((host.to_string(), port))
}
