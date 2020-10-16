
use tokio::prelude::*;

use clap::App;
use structopt::StructOpt;
use rdkafka::config::{ClientConfig, FromClientConfig};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication, TopicResult};
use std::time::Duration;
use rdkafka::client::DefaultClientContext;
use rdkafka::error::KafkaResult;
use rdkafka::metadata::Metadata;
use dialoguer::theme::{ColorfulTheme, Theme};
use dialoguer::{Select, Input};

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

const TIMEOUT: Duration = Duration::from_secs(30);
const WAVING_HAND_EMOJI: char = '\u{1F44B}';

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opt = Opt::from_args();
    println!("Opt {:?}", opt.brokers);

    let theme = ColorfulTheme::default();
    //
    // let base_consumer = create_consumer_client(&opt);
    // let metadata = get_metadata(&base_consumer);

    loop {
        let cmd: String = Input::with_theme(&theme)
            .default("?".parse().unwrap())
            .interact()
            .expect("Unable to get command");

        match cmd.as_str() {
            "lb" => list_brokers(&opt),
            "lt" => list_topics(&opt),
            "ct" => create_topic_interactive(&opt, &theme).await,
            "exit" => {
                println!("Goodbye {} !", WAVING_HAND_EMOJI);
                break
            },
            "help" | "?" | _ => print_help()
        }
    }

    Ok(())

    // let client = create_admin_client(&opt);
    // task::block_on(create_topic("bite", 1,1, &client));


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

fn print_help() {
    println!("Type commands : ");
    println!("lb : to list brokers");
    println!("lt : to list topics");
    println!("ct : to create topic");
    println!("help | ? : to display help");
    println!("exit : quit shell");
}

fn list_brokers(options: &Opt) -> () {
    let base_consumer = create_consumer_client(options);
    let metadata = get_metadata(&base_consumer);
    println!("Brokers:");
    for broker in metadata.brokers() {
        println!(
            "  Id: {}  Host: {}:{}  ",
            broker.id(),
            broker.host(),
            broker.port()
        );
    }
}


fn list_topics(options: &Opt) -> () {
    let base_consumer = create_consumer_client(options);
    let metadata = get_metadata(&base_consumer);
    println!("Topics:");
    for topic in metadata.topics() {
        println!(
            "  Name: {}  Partitions: {}",
            topic.name(),
            topic.partitions().len()
        );
    }
}

fn client_config(options: &Opt) -> ClientConfig {
    let mut connard = ClientConfig::new();
    connard.set("bootstrap.servers", options.brokers.first().unwrap().as_str());
    connard
}

fn create_admin_client(options: &Opt) -> AdminClient<DefaultClientContext> {
    AdminClient::from_config(&client_config(options)).expect("Fail to create admin client")
}

fn create_consumer_client(options: &Opt) -> BaseConsumer {
    client_config(options)
        .create()
        .expect("Consumer creation failed")
}

async fn create_topic_interactive(options: &Opt, theme: &dyn Theme) -> () {
    let name: String = Input::with_theme(theme)
        .with_prompt("Topic name : ")
        .interact()
        .expect("Unable to get topic name");

    let num_partitions: i32 = Input::with_theme(theme)
        .with_prompt("Partitions number : ")
        .default(1)
        .interact()
        .expect("Unable to get partition number");

    let replication_factor: i32 = Input::with_theme(theme)
        .with_prompt("Replication factor : ")
        .default(1)
        .interact()
        .expect("Unable to get replication factor");

    let admin = AdminClient::from_config(&client_config(options)).expect("Fail to create admin client");
    let admin_opts = AdminOptions::new().operation_timeout(Some(TIMEOUT));
    let new_topic = NewTopic::new(name.as_str(), num_partitions, TopicReplication::Fixed(replication_factor));
    println!("Create topic {} {}:{}", name, num_partitions, replication_factor);
    let res = admin.create_topics(vec!(&new_topic), &admin_opts).await.expect("topic creation failed");
    println!("Topic created : {:?}", res);

}

// async fn _create_topic(topic_name: &str, num_partitions: i32, replication_factor: i32, client: &AdminClient<DefaultClientContext>) -> Vec<TopicResult> {
//     let new_topic = NewTopic::new(topic_name, num_partitions, TopicReplication::Fixed(replication_factor));
//     client.create_topics(vec!(&new_topic), &AdminOptions::new()).await.expect("Failed create topic")
// }

fn get_metadata(consumer: &BaseConsumer) -> Metadata {
    consumer
        .fetch_metadata(None, TIMEOUT)
        .expect("Failed to fetch metadata")
}
