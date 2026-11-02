rootProject.name = "prodcheck"

dependencyResolutionManagement {
    repositories {
        mavenLocal {
            content { includeGroup("io.github.dsudomoin") } // the library under test, published from this checkout
        }
        mavenCentral()
    }
}

include("common", "target", "shop", "courier", "accounts")
