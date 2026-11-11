/*
 * Copyright (c) 2023 Adaptive Financial Consulting
 */

plugins {
    application
    checkstyle
}

repositories {
    mavenCentral()
}

dependencies {
    checkstyle(libs.checkstyle)
    implementation(libs.agrona)
    implementation(libs.slf4j)
    implementation(libs.logback)
    implementation(project(":sbe-protocol"))
    testImplementation(libs.bundles.testing)
}

testing {
    suites {
        // Configure the built-in test suite
        @Suppress("UNUSED_VARIABLE")
        val test = getByName<JvmTestSuite>("test") {
            // Use JUnit Jupiter test framework
            useJUnitJupiter(libs.versions.junitVersion.get())

            targets {
                all {
                    testTask {
                        jvmArgs("--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED")
                    }
                }
            }
        }
    }
}

