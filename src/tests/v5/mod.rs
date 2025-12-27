mod connect;
mod connack;
mod publish;
mod subscribe;
mod unsubscribe;
mod ping;
mod disconnect;
mod data;
mod topics;
mod operational;
mod properties;
mod session;
mod shared;
mod qos;
mod will;
mod auth;

use super::NormativeTest;

pub fn get_tests() -> Vec<NormativeTest> {
    let mut tests = Vec::new();
    tests.extend(connect::get_tests());
    tests.extend(connack::get_tests());
    tests.extend(publish::get_tests());
    tests.extend(subscribe::get_tests());
    tests.extend(unsubscribe::get_tests());
    tests.extend(ping::get_tests());
    tests.extend(disconnect::get_tests());
    tests.extend(data::get_tests());
    tests.extend(topics::get_tests());
    tests.extend(operational::get_tests());
    tests.extend(properties::get_tests());
    tests.extend(session::get_tests());
    tests.extend(shared::get_tests());
    tests.extend(qos::get_tests());
    tests.extend(will::get_tests());
    tests.extend(auth::get_tests());
    tests
}
