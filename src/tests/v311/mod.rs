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
mod fixedheader;
mod variableheader;
mod pubrel;

use super::NormativeTest;

pub fn get_tests() -> Vec<NormativeTest> {
    let mut tests = Vec::new();
    tests.extend(data::get_tests());
    tests.extend(fixedheader::get_tests());
    tests.extend(variableheader::get_tests());
    tests.extend(connect::get_tests());
    tests.extend(connack::get_tests());
    tests.extend(publish::get_tests());
    tests.extend(pubrel::get_tests());
    tests.extend(subscribe::get_tests());
    tests.extend(unsubscribe::get_tests());
    tests.extend(ping::get_tests());
    tests.extend(disconnect::get_tests());
    tests.extend(topics::get_tests());
    tests.extend(operational::get_tests());
    tests
}
