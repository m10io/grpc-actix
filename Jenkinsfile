node('ec2') {
  checkout scm
  stage('Build') {
    sshagent (credentials: ['fd52c993-fb69-43d2-95fd-62a6a89baf0e']) {
      dir('grpc-actix') {
        sh '/home/ubuntu/.cargo/bin/cargo build'
        sh '/home/ubuntu/.cargo/bin/cargo build --features timing'
      }
      dir('grpc-actix-build') {
        sh '/home/ubuntu/.cargo/bin/cargo build'
      }
    }
  }
  try {
    stage('Test') {
      dir('grpc-actix') {
        sh '/home/ubuntu/.cargo/bin/cargo test | /home/ubuntu/.cargo/bin/cargo_test_formatter > ../test-default.xml'
        sh '/home/ubuntu/.cargo/bin/cargo test --features timing | /home/ubuntu/.cargo/bin/cargo_test_formatter > ../test-timing.xml'
      }
    }
  } finally {
    junit 'test-default.xml'
    junit 'test-timing.xml'
  }
  stage('Clippy') {
    dir('grpc-actix') {
      sh '/home/ubuntu/.cargo/bin/cargo +nightly clippy --all -- -Dwarnings'
      sh '/home/ubuntu/.cargo/bin/cargo +nightly clippy --all --features timing -- -Dwarnings'
    }
    dir('grpc-actix-build') {
      sh '/home/ubuntu/.cargo/bin/cargo +nightly clippy --all -- -Dwarnings'
    }
  }
  stage('Format') {
    sh '/home/ubuntu/.cargo/bin/cargo +nightly fmt -- --check'
  }
}
