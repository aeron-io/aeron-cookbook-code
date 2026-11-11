
plugins {
    application
    checkstyle
}

@Suppress("DEPRECATION")
val generatedDir = file("${buildDir}/generated/src/main/java")
val codecGeneration = configurations.create("codecGeneration")

dependencies {
    "codecGeneration"(libs.sbe)
    checkstyle(libs.checkstyle)
    implementation(libs.aeron.samples)
    implementation(libs.agrona)
    implementation(libs.slf4j)
    implementation(libs.logback)
    testImplementation(libs.aeron.samples)
    testImplementation(libs.agrona)
    testImplementation(libs.bundles.testing)
}

sourceSets {
    main {
        java.srcDirs("src/main/java", generatedDir)
    }
}

testing {
    suites {
        // Configure the built-in test suite
        @Suppress("UNUSED_VARIABLE")
        val test = getByName<JvmTestSuite>("test") {
            // Use JUnit Jupiter test framework
            useJUnitJupiter(libs.versions.junitVersion.get())
        }
    }
}


tasks {

    register("generateCodecs", JavaExec::class) {
        group = "sbe"
        val codecsFile = "src/main/resources/messages.xml"
        val sbeFile = "src/main/resources/sbe/sbe.xsd"
        inputs.files(codecsFile, sbeFile)
        outputs.dir(generatedDir)
        classpath = codecGeneration
        jvmArgs("--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED")
        mainClass.set("uk.co.real_logic.sbe.SbeTool")
        args = listOf(codecsFile)
        systemProperties["sbe.output.dir"] = generatedDir
        systemProperties["sbe.target.language"] = "Java"
        systemProperties["sbe.validation.xsd"] = sbeFile
        systemProperties["sbe.validation.stop.on.error"] = "true"
        outputs.dir(generatedDir)
    }

    register ("uberRsmCluster", Jar::class) {
        group = "uber"
        manifest {
            attributes["Main-Class"]="com.aeroncookbook.cluster.rsm.node.RsmCluster"
        }
        archiveClassifier.set("uber-cluster")
        from(sourceSets.main.get().output)
        duplicatesStrategy = DuplicatesStrategy.EXCLUDE
        dependsOn(configurations.runtimeClasspath)
        dependsOn(compileJava)
        from({
            configurations.runtimeClasspath.get().filter { it.name.endsWith("jar") }.map { zipTree(it) }
        })
    }


    register ("uberRsmClient", Jar::class) {
        group = "uber"
        manifest {
            attributes["Main-Class"]="com.aeroncookbook.cluster.rsm.client.ClusterClient"
        }
        archiveClassifier.set("uber-client")
        from(sourceSets.main.get().output)
        duplicatesStrategy = DuplicatesStrategy.EXCLUDE
        dependsOn(configurations.runtimeClasspath)
        dependsOn(compileJava)
        from({
            configurations.runtimeClasspath.get().filter { it.name.endsWith("jar") }.map { zipTree(it) }
        })
    }

    compileJava {
        dependsOn("generateCodecs")
    }
}
