pipeline {
    agent {
        docker {
            image 'maven:3.8.1-amazoncorretto-11'
            args '-v /root/.m2:/root/.m2'
        }
    }
    triggers {
        cron('@midnight')
    }
    stages {
        stage('Build') {
            steps {
                sh 'mvn -B -DskipTests clean package'
            }
        }
        stage('Test') {
            steps {
                sh 'mvn test'
            }
            post {
                always {
                    junit 'target/surefire-reports/*.xml'
                }
            }
        }
        stage('Coverage') {
            steps {
                sh 'mvn jacoco:prepare-agent install jacoco:report'
                jacoco(
                  execPattern: 'target/*.exec',
                  classPattern: 'target/classes',
                  sourcePattern: 'src/main/java',
                  exclusionPattern: 'src/test*'
                )
            }
        }
    }
}
