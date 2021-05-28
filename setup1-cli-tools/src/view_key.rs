use snarkos_toolkit::account::{PrivateKey, ViewKey};

fn main() {
    let mut rng = rand::thread_rng();
    let private_key = PrivateKey::new(&mut rng).expect("Should generate a random PrivateKey struct");
    let view_key = ViewKey::from(&private_key).expect("Should create a ViewKey from a PrivateKey");
    print!("{}", view_key);
}
