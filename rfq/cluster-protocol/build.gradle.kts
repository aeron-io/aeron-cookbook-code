/*
 * Copyright 2023 Adaptive Financial Consulting
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
        val codecsFile = "src/main/resources/protocol/protocol-codecs.xml"
        val sbeFile = "src/main/resources/protocol/fpl/sbe.xsd"
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

    compileJava {
        dependsOn("generateCodecs")
    }
}
