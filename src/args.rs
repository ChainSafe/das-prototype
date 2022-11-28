use clap::{Args, Parser, Subcommand};
use strum::EnumString;

#[derive(Clone, Debug, PartialEq, EnumString)]
pub enum Topology {
    #[strum(serialize = "linear", serialize = "1")]
    Linear,
    #[strum(serialize = "uniform", serialize = "2")]
    Uniform,
}

#[derive(Clone, Debug, PartialEq, EnumString)]
pub enum TalkWire {
    #[strum(serialize = "discv5")]
    Discv5,
    #[strum(serialize = "libp2p")]
    Libp2p,
}

// Defines settings how samples are forwarded when redundancy is enabled
#[derive(Clone, Debug, PartialEq, EnumString)]
pub enum ForwardPolicy {
    // forward only the first message of the type
    #[strum(serialize = "F1")]
    ForwardOne,
    // forward every time the message is received
    #[strum(serialize = "FA")]
    ForwardAll,
}

// Defines settings how samples are replicated when redundancy is enabled
#[derive(Clone, Debug, PartialEq, EnumString)]
pub enum ReplicatePolicy {
    // replicate only at the dispersal initiator
    #[strum(serialize = "R1")]
    ReplicateOne,
    // replicate at every step so a receiving node would also forward messages to certain number (based on --redundancy) of nodes in corresponding k-bucket
    #[strum(serialize = "RS")]
    ReplicateSome,
    // replicate at every step so a receiving node would also forward messages to every node in corresponding k-bucket
    #[strum(serialize = "RA")]
    ReplicateAll,
}

#[derive(Clone, Debug, PartialEq, EnumString)]
pub enum BatchingStrategy {
    #[strum(serialize = "b", serialize = "bucket-wise")]
    BucketWise,
    /// note: must be used with `ForwardPolicy::ForwardAll` could be a bug \_(*_*)_/
    #[strum(serialize = "d", serialize = "distance-wise")]
    DistanceWise,
}

#[derive(Clone, Debug, PartialEq, EnumString)]
pub enum RoutingStrategy {
    #[strum(serialize = "i", serialize = "iterative")]
    Iterative,
    #[strum(serialize = "r", serialize = "recursive")]
    Recursive,
}

#[derive(Clone, Debug, PartialEq, EnumString)]
pub enum LookupMethod {
    #[strum(serialize = "v", serialize = "find-value")]
    Discv5FindValue,
    #[strum(serialize = "c", serialize = "find-content")]
    OverlayFindContent,
}

#[derive(Clone, Parser)]
pub struct Options {
    #[clap(long, short, default_value = "127.0.0.1")]
    pub ip_listen: String,
    #[clap(long, short, default_value = "9000")]
    pub port_udp: usize,
    #[clap(long, short, default_value = "10")]
    pub node_count: usize,
    #[clap(long, short, default_value = "linear")]
    pub topology: Topology,
    #[clap(long, short, default_value = "discv5")]
    pub wire_protocol: TalkWire,
    #[clap(long = "timeout", default_value = "3")]
    pub request_timeout: u64,
    #[clap(long, short, default_value = "./data")]
    pub cache_dir: String,
    #[clap(long, default_value = "new")]
    pub snapshot: String,

    #[command(subcommand)]
    pub simulation_case: SimulationCase,
}

#[derive(Clone, clap::Subcommand)]
pub enum SimulationCase {
    Disseminate(DisseminationArgs),
    Sample(SamplingArgs),
}

#[derive(Clone, Args)]
pub struct DisseminationArgs {
    #[clap(long, short, default_value = "256")]
    pub number_of_samples: usize,
    #[clap(long, default_value = "F1")]
    pub forward_mode: ForwardPolicy,
    #[clap(long, default_value = "R1")]
    pub replicate_mode: ReplicatePolicy,
    #[clap(long, short, default_value = "1")]
    pub redundancy: usize,
    #[clap(long, short = 'b', default_value = "b")]
    pub batching_strategy: BatchingStrategy,
    #[clap(long, short = 's', default_value = "r")]
    pub routing_strategy: RoutingStrategy,
    #[clap(long, short, default_value = "15")]
    pub parallelism: usize,
}

#[derive(Clone, Args)]
pub struct SamplingArgs {
    #[clap(flatten)]
    pub dissemination_args: DisseminationArgs,

    #[clap(long, short = 'k', default_value = "75")]
    pub samples_per_validator: usize,

    #[clap(long, short)]
    pub validators_number: usize,

    #[clap(long, short, default_value = "15")]
    pub parallelism: usize,

    #[clap(long, short, default_value = "find-value")]
    pub lookup_method: LookupMethod,
}
