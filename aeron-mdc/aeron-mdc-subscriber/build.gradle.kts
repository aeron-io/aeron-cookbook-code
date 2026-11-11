
plugins {
    application
    checkstyle
}

dependencies {
    checkstyle(libs.checkstyle)
    implementation(libs.agrona)
    implementation(libs.aeron.archive)
    implementation(libs.slf4j)
    implementation(libs.logback)
    testImplementation(libs.bundles.testing)
}

tasks {

    register("uberJar", Jar::class) {
        group = "uber"
        manifest {
            attributes["Main-Class"] = "com.aeroncookbook.aeron.mdc.MultiDestinationSubscriber"
        }
        archiveClassifier.set("uber")
        from(sourceSets.main.get().output)
        duplicatesStrategy = DuplicatesStrategy.EXCLUDE
        dependsOn(configurations.runtimeClasspath)
        from({
            configurations.runtimeClasspath.get().filter { it.name.endsWith("jar") }.map { zipTree(it) }
        })
    }
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
                        jvmArgs(
                            "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
                            "--add-opens", "java.base/java.util.zip=ALL-UNNAMED")
                    }
                }
            }
        }
    }
}
