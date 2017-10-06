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
                parallel(
                    Project1: { sh "./gradlew :project-1:clean assemble" },
                    Project2: { sh "./gradlew :project-2:clean assemble" },
                    Project3: { sh "./gradlew :project-3:clean assemble" },
                )
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

                jacoco exclusionPattern: '**/classes/java/test/**/*.class', sourcePattern: '**/src/'

                archiveArtifacts artifacts: '*/build/libs/*.jar'

                zip zipFile: 'coverage.zip', glob: '*/build/reports/jacoco/', archive: true

                sh "rm -rf */build/reports/"
            }
        }
    }
}
