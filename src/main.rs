use clap::App;
use structopt::StructOpt;

use rdkafka::config::{ClientConfig, FromClientConfig};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use std::time::Duration;
use rdkafka::client::DefaultClientContext;

#[derive(StructOpt, Debug)]
#[structopt(name = "kafka-shell", about = "Interactive shell for kafka.")]
struct Opt {
    /// Bootstrap servers
    #[structopt(short, long)]
    brokers: Vec<String>,

    // The number of occurrences of the `v/verbose` flag
    /// Verbose mode (-v, -vv, -vvv, etc.)
    #[structopt(short, long, parse(from_occurrences))]
    verbose: u8,
}


fn main() {
   let opt = Opt::from_args();
    println!("Opt {:?}", opt.brokers);
    let client = create_consumer_client(&opt);

    // admin.create_topics(vec!(&NewTopic::new("test", 2, TopicReplication::Fixed(2))), &AdminOptions::new());
    //
    // let consumer: BaseConsumer = ClientConfig::new()
    //     .set("bootstrap.servers", "192.168.96.3:9092")
    //     .create()
    //     .expect("Consumer creation failed");
    //
    // let metadata = consumer
    //     .fetch_metadata(Some("test"), Duration::from_secs(30))
    //     .expect("Failed to fetch metadata");
    //
    // println!("Brokers:");
    // for broker in metadata.brokers() {
    //     println!(
    //         "  Id: {}  Host: {}:{}  ",
    //         broker.id(),
    //         broker.host(),
    //         broker.port()
    //     );
    // }
}

fn client_config(options: &Opt) -> &mut ClientConfig {
    let bootstrap_servers: &str = options.brokers.first().unwrap().as_str();
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
}

fn create_admin_client(options: &Opt) -> AdminClient<DefaultClientContext> {
    AdminClient::from_config(&client_config(options)).expect("Fail to create admin client")
}

fn create_consumer_client(options: &Opt) {
    client_config(options)
        .create()
        .expect("Consumer creation failed");
}

async fn create_topic(client: AdminClient<DefaultClientContext>) {
    let topic_name = "test";
    let num_partitions = 1;
    let replication_factor = 1;

    let new_topic = NewTopic::new(topic_name, num_partitions, TopicReplication::Fixed(replication_factor));

    client.create_topics(vec!(&new_topic), &AdminOptions::new());
}
