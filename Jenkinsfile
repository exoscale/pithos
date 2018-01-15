@Library('jenkins-pipeline') _

node {
  // Wipe the workspace so we are building completely clean
  cleanWs()

  try {
    dir('src') {
      stage('checkout code') {
        checkout scm
      }
      lein()
      build()
      archiveArtifacts artifacts: '**/target/*.jar', fingerprint: true
    }
    stage('upload') {
      //aptlyUpload('staging', 'xenial', 'main', 'build-area/*deb')
    }
  }
  catch (err) {
    currentBuild.result = 'FAILED'
    ircNotification()
    throw err
  }

  finally {
    if (currentBuild.result != 'FAILURE') {
      ircNotification(
    }
  }
}

def lein() {
  stage('lein') {
    def clojureContainer = docker.image('exoscale/clojure:2.6.1')
    clojureContainer.inside('-u root') {
      sh 'lein clean'
      sh 'lein compile :all'
      sh 'lein jar'
      sh 'lein pom'
      sh 'lein fatdeb'
    }
  }
}

def build() {
  stage('build') {
    def mavenContainer = docker.image('exoscale/maven:latest')
    mavenContainer.inside('-u root') {
      sh 'mvn install'
    }
  }
}

