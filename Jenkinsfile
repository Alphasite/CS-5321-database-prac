void setBuildStatus(String message, String state) {
  step([
      $class: "GitHubCommitStatusSetter",
      reposSource: [$class: "ManuallyEnteredRepositorySource", url: "https://github.com/my-org/my-repo"],
      contextSource: [$class: "ManuallyEnteredCommitContextSource", context: "ci/jenkins/build-status"],
      errorHandlers: [[$class: "ChangingBuildStatusErrorHandler", result: "UNSTABLE"]],
      statusResultSource: [ $class: "ConditionalStatusResultSource", results: [[$class: "AnyBuildResult", message: message, state: state]] ]
  ]);
}

pipeline {
    agent any

    stages {
        stage('Preparation') {
            steps {
                checkout scm
                sh "rm -f pit-test.zip"
                sh "rm -f coverage.zip"
                sh "chmod +x gradlew"
            }
        }

        stage('Build') {
            steps {
                sh "./gradlew clean assemble"
            }
        }

        stage('Test') {
            steps {
                parallel(
                    Project1: { sh "./gradlew :project-1:test jacocoTestReport" },
                    Project2: { sh "./gradlew :project-2:test jacocoTestReport" },
                    Project3: { sh "./gradlew :project-3:test jacocoTestReport" },
                )            
            }
        }

        stage('Findbugs') {
            steps {
                parallel(
                    Project1: { sh "./gradlew :project-1:findbugsMain" },
                    Project2: { sh "./gradlew :project-2:findbugsMain" },
                    Project3: { sh "./gradlew :project-3:findbugsMain" },
                )
            }
        }

        stage('Results') {
            steps {
                junit '**/test-results/test/TEST-*.xml'
                setBuildStatus("Build complete", "SUCCESS");

                archiveArtifacts artifacts: '*/build/distributions/*.zip'

                zip zipFile: 'coverage.zip', glob: '*/build/reports/jacoco/', archive: true

                sh "rm -rf */build/reports/"
            }
        }
    }
}
