use serde::{Deserialize, Serialize};

use crate::{RaftError, Result};

/// Unique ID of a server in the cluster.
pub type ServerID = String;
/// Network address of a server, understood by the transport.
pub type ServerAddress = String;

/// Whether a server has a vote. Mirrors `ServerSuffrage` in configuration.go.
/// The deprecated `Staging` variant is not ported.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ServerSuffrage {
    /// A server whose vote counts and whose logs are replicated.
    Voter,
    /// A server that receives logs but does not vote.
    Nonvoter,
}

/// A single server in the cluster configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Server {
    pub suffrage: ServerSuffrage,
    pub id: ServerID,
    pub address: ServerAddress,
}

/// The set of servers in the cluster.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Configuration {
    pub servers: Vec<Server>,
}

impl Configuration {
    /// Returns true if the server has a vote in this configuration.
    pub fn has_vote(&self, id: &str) -> bool {
        self.servers
            .iter()
            .any(|s| s.suffrage == ServerSuffrage::Voter && s.id == id)
    }

    /// Returns true if the server is present in this configuration, with any
    /// suffrage. Mirrors `inConfiguration` in configuration.go.
    pub fn contains(&self, id: &str) -> bool {
        self.servers.iter().any(|s| s.id == id)
    }
}

/// Encodes a configuration for storage in a log entry or snapshot.
pub fn encode_configuration(conf: &Configuration) -> Result<Vec<u8>> {
    Ok(rmp_serde::to_vec_named(conf)?)
}

/// Decodes a configuration from a log entry or snapshot.
pub fn decode_configuration(buf: &[u8]) -> Result<Configuration> {
    Ok(rmp_serde::from_slice(buf)?)
}

/// Validates a configuration: non-empty IDs and addresses, no duplicate
/// IDs or addresses, at least one voter.
pub fn check_configuration(conf: &Configuration) -> Result<()> {
    for server in &conf.servers {
        if server.id.is_empty() {
            return Err(RaftError::Configuration("empty server ID".into()));
        }
        if server.address.is_empty() {
            return Err(RaftError::Configuration("empty server address".into()));
        }
    }
    let mut ids = std::collections::HashSet::new();
    let mut addrs = std::collections::HashSet::new();
    let mut voters = 0;
    for server in &conf.servers {
        if !ids.insert(&server.id) {
            return Err(RaftError::Configuration(format!(
                "found duplicate ID in servers: {}",
                server.id
            )));
        }
        if !addrs.insert(&server.address) {
            return Err(RaftError::Configuration(format!(
                "found duplicate address in servers: {}",
                server.address
            )));
        }
        if server.suffrage == ServerSuffrage::Voter {
            voters += 1;
        }
    }
    if voters == 0 {
        return Err(RaftError::Configuration(
            "need at least one voter in configuration".into(),
        ));
    }
    Ok(())
}

/// A membership change command. Mirrors `ConfigurationChangeCommand`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigurationChangeCommand {
    AddVoter,
    AddNonvoter,
    DemoteVoter,
    RemoveServer,
}

/// A request to change the cluster configuration, internal to the library.
#[derive(Debug, Clone)]
pub struct ConfigurationChangeRequest {
    pub command: ConfigurationChangeCommand,
    pub server_id: ServerID,
    pub server_address: ServerAddress,
    /// Only allow the change if the previous configuration index matches.
    /// Zero disables the check.
    pub prev_index: u64,
}

/// Computes the configuration that results from applying a change to the
/// current configuration. Mirrors `nextConfiguration` in configuration.go.
///
/// `current_index` is the index of the current configuration; the returned
/// configuration is assigned `current_index + 1` by the caller.
pub fn next_configuration(
    current: &Configuration,
    change: &ConfigurationChangeRequest,
) -> Result<Configuration> {
    let mut next = current.clone();
    match change.command {
        ConfigurationChangeCommand::AddVoter => {
            let mut found = false;
            for server in &mut next.servers {
                if server.id == change.server_id {
                    server.suffrage = ServerSuffrage::Voter;
                    if !change.server_address.is_empty() {
                        server.address = change.server_address.clone();
                    }
                    found = true;
                }
            }
            if !found {
                next.servers.push(Server {
                    suffrage: ServerSuffrage::Voter,
                    id: change.server_id.clone(),
                    address: change.server_address.clone(),
                });
            }
        }
        ConfigurationChangeCommand::AddNonvoter => {
            let mut found = false;
            for server in &mut next.servers {
                if server.id == change.server_id {
                    server.suffrage = ServerSuffrage::Nonvoter;
                    if !change.server_address.is_empty() {
                        server.address = change.server_address.clone();
                    }
                    found = true;
                }
            }
            if !found {
                next.servers.push(Server {
                    suffrage: ServerSuffrage::Nonvoter,
                    id: change.server_id.clone(),
                    address: change.server_address.clone(),
                });
            }
        }
        ConfigurationChangeCommand::DemoteVoter => {
            for server in &mut next.servers {
                if server.id == change.server_id {
                    server.suffrage = ServerSuffrage::Nonvoter;
                }
            }
        }
        ConfigurationChangeCommand::RemoveServer => {
            next.servers.retain(|s| s.id != change.server_id);
        }
    }
    check_configuration(&next)?;
    Ok(next)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(cmd: ConfigurationChangeCommand, id: &str, addr: &str) -> ConfigurationChangeRequest {
        ConfigurationChangeRequest {
            command: cmd,
            server_id: id.to_string(),
            server_address: addr.to_string(),
            prev_index: 0,
        }
    }

    fn voter(id: &str, addr: &str) -> Server {
        Server {
            suffrage: ServerSuffrage::Voter,
            id: id.to_string(),
            address: addr.to_string(),
        }
    }

    #[test]
    fn add_voter_appends_new_server() {
        let conf = Configuration {
            servers: vec![voter("a", "addr-a")],
        };
        let next = next_configuration(
            &conf,
            &req(ConfigurationChangeCommand::AddVoter, "b", "addr-b"),
        )
        .unwrap();
        assert_eq!(next.servers.len(), 2);
        assert!(next.has_vote("b"));
    }

    #[test]
    fn remove_server_drops_it() {
        let conf = Configuration {
            servers: vec![voter("a", "addr-a"), voter("b", "addr-b")],
        };
        let next = next_configuration(
            &conf,
            &req(ConfigurationChangeCommand::RemoveServer, "b", ""),
        )
        .unwrap();
        assert_eq!(next.servers.len(), 1);
        assert!(!next.servers.iter().any(|s| s.id == "b"));
    }

    #[test]
    fn demote_voter_keeps_server_without_vote() {
        let conf = Configuration {
            servers: vec![voter("a", "addr-a"), voter("b", "addr-b")],
        };
        let next = next_configuration(
            &conf,
            &req(ConfigurationChangeCommand::DemoteVoter, "b", ""),
        )
        .unwrap();
        assert_eq!(next.servers.len(), 2);
        assert!(!next.has_vote("b"));
    }

    #[test]
    fn check_configuration_rejects_no_voters() {
        let conf = Configuration {
            servers: vec![Server {
                suffrage: ServerSuffrage::Nonvoter,
                id: "a".into(),
                address: "addr-a".into(),
            }],
        };
        assert!(check_configuration(&conf).is_err());
    }

    #[test]
    fn check_configuration_rejects_duplicate_ids() {
        let conf = Configuration {
            servers: vec![voter("a", "addr-a"), voter("a", "addr-b")],
        };
        assert!(check_configuration(&conf).is_err());
    }

    #[test]
    fn configuration_msgpack_roundtrip() {
        let conf = Configuration {
            servers: vec![voter("a", "addr-a")],
        };
        let buf = encode_configuration(&conf).unwrap();
        let decoded = decode_configuration(&buf).unwrap();
        assert_eq!(conf, decoded);
    }
}
