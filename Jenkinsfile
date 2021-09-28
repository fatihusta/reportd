void buildReportd(String libc, String buildDir) {
    sh "docker pull untangleinc/reportd:build-${libc}"
    sh "docker-compose -f ${buildDir}/build/docker-compose.build.yml -p reportd_${libc} run ${libc}-local"
    sh "cp ${buildDir}/cmd/reportd/reportd cmd/reportd/reportd-${libc}"
}

void archiveReportd() {
    archiveArtifacts artifacts:'cmd/reportd/reportd*', fingerprint: true
}

pipeline {
    agent none

    stages {
        stage('Build') {
            parallel {
                stage('Build musl') {
                    agent { label 'docker' }

                    environment {
                        libc = 'musl'
                        buildDir = "${env.HOME}/build-reportd-${env.BRANCH_NAME}-${libc}/go/src/github.com/untangle/reportd"
                    }

                    stages {
                        stage('Prep WS musl') {
                            steps { dir(buildDir) { checkout scm } }
                        }

                        stage('Build reportd musl') {
                            steps {
                                buildReportd(libc, buildDir)
                                stash(name:"reportd-${libc}", includes:"cmd/reportd/reportd*")
                            }
                        }
                    }

                    post {
                        success { archiveReportd() }
                    }
                }

                stage('Build glibc') {
                    agent { label 'docker' }

                    environment {
                        libc = 'glibc'
                        buildDir = "${env.HOME}/build-reportd-${env.BRANCH_NAME}-${libc}/go/src/github.com/untangle/reportd"
                    }

                    stages {
                        stage('Prep WS glibc') {
                            steps { dir(buildDir) { checkout scm } }
                        }

                        stage('Build reportd glibc') {
                            steps {
                                buildReportd(libc, buildDir)
                                stash(name:"reportd-${libc}", includes:'cmd/reportd/reportd*')
                            }
                        }
                    }

                    post {
                        success { archiveReportd() }
                    }
                }
            }
        }

        stage('Test') {
            parallel {
                stage('Test musl') {
                    agent { label 'docker' }

                    environment {
                        libc = 'musl'
                        reportd = "cmd/reportd/reportd-${libc}"
                    }

                    stages {
                        stage('Prep musl') {
                            steps {
                                unstash(name:"reportd-${libc}")
                            }
                        }

                        stage('File testing for musl') {
                            steps {
                                sh "test -f ${reportd} && file ${reportd} | grep -v -q GNU/Linux"
                            }
                        }
                    }
                }

                stage('Test libc') {
                    agent { label 'docker' }

                    environment {
                        libc = 'glibc'
                        reportd = "cmd/reportd/reportd-${libc}"
                        dockerfile = 'build/docker-compose.test.yml'
                    }

                    stages {
                        stage('Prep libc') {
                            steps {
                                unstash(name:"reportd-${libc}")
                            }
                        }

                        stage('File testing for libc') {
                            steps {
                                sh "test -f ${reportd} && file ${reportd} | grep -q GNU/Linux"
                            }
                        }

                        stage('settingsd testing for libc') {
                            steps {
                                sh "docker-compose -f ${dockerfile} build local"
                                sh "docker-compose -f ${dockerfile} up --abort-on-container-exit --exit-code-from local"
                            }
                        }
                    }
                }
            }

            post {
                changed {
                    script {
                        // set result before pipeline ends, so emailer sees it
                        currentBuild.result = currentBuild.currentResult
                    }
                    emailext(to:'nfgw-engineering@untangle.com', subject:"${env.JOB_NAME} #${env.BUILD_NUMBER}: ${currentBuild.result}", body:"${env.BUILD_URL}")
                    slackSend(channel:'#team_engineering', message:"${env.JOB_NAME} #${env.BUILD_NUMBER}: ${currentBuild.result} at ${env.BUILD_URL}")
                }
            }
        }
    }
}
