node('ec2') {
  checkout scm
  stage('Build') {
    sshagent (credentials: ['fd52c993-fb69-43d2-95fd-62a6a89baf0e']) {
        sh '/home/ubuntu/.cargo/bin/cargo build'
    }
  }
  stage('Test') {
    sh '/home/ubuntu/.cargo/bin/cargo test'
  }
  stage('Clippy') {
    sh '/home/ubuntu/.cargo/bin/cargo +nightly clippy --all'
  }
}
