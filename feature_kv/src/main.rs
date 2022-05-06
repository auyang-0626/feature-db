use std::error::Error;

use log::info;

mod init;
mod operate;
mod meta_client;
mod node;

fn main(){
    init::init();

}
