pipeline {
    agent any

    stages {
        stage('Preparation') {
            steps {
                checkout scm
                sh "rm -f pit-test.zip"
                sh "rm -f coverage.zip"
                sh "chmod +x gradlew"
                sh "./gradlew clean"
            }
        }

        stage('Build') {
            steps {
                parallel(
                    Project1: { sh "./gradlew :project-1:assemble" },
                    Project2: { sh "./gradlew :project-2:assemble" },
                    Project3: { sh "./gradlew :project-3:assemble" },
                )
            }
        }

        stage('Test') {
            steps {
                parallel(
                    Project1: { sh "./gradlew :project-1:test" },
                    Project2: { sh "./gradlew :project-2:test" },
                    Project3: { sh "./gradlew :project-3:test" },
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
                archiveArtifacts artifacts: '*/build/libs/*.jar'
            }
        }
    }

    post {
        always {
            sh "./gradlew jacocoTestReport"

            junit '**/test-results/test/TEST-*.xml'
            jacoco exclusionPattern: '**/classes/java/test/**/*.class', sourcePattern: '**/src/'
            zip zipFile: 'coverage.zip', glob: '*/build/reports/jacoco/', archive: true

            sh "rm -rf */build/reports/"
        }
        failure {

        }
    }
}
