plugins {
    `java-library`
}

@Suppress("DEPRECATION")
val generatedDir = file("${buildDir}/generated/src/main/java")
val codecGeneration = configurations.create("codecGeneration")

dependencies {
    "codecGeneration"(libs.sbe)
    implementation(libs.agrona)
    testImplementation(libs.bundles.testing)
}

sourceSets {
    main {
        java.srcDir(generatedDir)
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

    register("generateCodecsWithPrecedeceChecks", JavaExec::class) {
        group = "sbe"
        val codecsFile = "src/main/resources/precedence-checked-messages.xml"
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
        systemProperties["sbe.generate.precedence.checks"] = "true"
        outputs.dir(generatedDir)
    }

    compileJava {
        dependsOn("generateCodecs")
        dependsOn("generateCodecsWithPrecedeceChecks")
    }
}
