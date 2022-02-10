# Harvest Web Service

Server application providing the functionality for capturing and indexing product metadata into the PDS Registry system. 
This application is different from the standalone Harvest Tool. 
It has to be used with other components, such as RabbitMQ message broker, Crawler Server and Harvest Client 
to enable performant ingestion of large data sets. 

* [📀 Installation](#---installation)
* [💁‍♀️ Usage](#------usage)
* [👥 Contributing](#---contributing)
  + [🔢 Versioning](#---versioning)
  + [Manual Publication](#manual-publication)
    - [Update Version Numbers](#update-version-numbers)
    - [Update Changelog](#update-changelog)
    - [Commit Changes](#commit-changes)
    - [Build and Deploy Software to Maven Central Repo](#build-and-deploy-software-to-maven-central-repo)
    - [Push Tagged Release](#push-tagged-release)
    - [Deploy Site to Github Pages](#deploy-site-to-github-pages)
    - [Update Versions For Development](#update-versions-for-development)
    - [Complete Release in Github](#complete-release-in-github)
* [📃 License](#---license)

## 📀 Installation

This is a Java application. You need Java 11 JDK and Maven to build it.
To create a binary distribution (ZIP and TGZ archives) run the following maven command:

```
mvn package
``` 

Binary archives (such as "registry-harvest-service-1.0.0-SNAPSHOT-bin.zip") will be created in "target" directory.

Prebuilt binaries are available in https://github.com/NASA-PDS/registry-harvest-service/releases

To install, just extract a binary archive into some folder, such as "/opt/harvest"


## 💁‍♀️ Usage

* Go to <install_directory>/bin and run "harvest-server" without any parameters to see usage information.

* This project includes documentation web application (maven site).
The documentation provides PDS Registry architecture overview, 
installation, and operation instructions.

To build and run local documentation web application execute the following maven command:

```
mvn site:run
```
Then open this URL in your web browser `http://localhost:8080`

* For more information about running all PDS Registry components in Docker see
https://github.com/NASA-PDS/registry


## 👥 Contributing

Within the NASA Planetary Data System, we value the health of our community as much as the code. Towards that end, we ask that you read and practice what's described in these documents:

-   Our [contributor's guide](https://github.com/NASA-PDS/.github/blob/main/CONTRIBUTING.md) delineates the kinds of contributions we accept.
-   Our [code of conduct](https://github.com/NASA-PDS/.github/blob/main/CODE_OF_CONDUCT.md) outlines the standards of behavior we practice and expect by everyone who participates with our software.


### 🔢 Versioning

We use the [SemVer](https://semver.org/) philosophy for versioning this software. Or not! Update this as you see fit.


### Manual Publication

_**NOTE: Requires using [PDS Maven Parent POM](https://github.com/NASA-PDS/pdsen-maven-parent) to ensure release profile is set.**_

#### Update Version Numbers

Update pom.xml for the release version or use the Maven Versions Plugin, e.g.:

```
# Skip this step if this is a RELEASE CANDIDATE, we will deploy as SNAPSHOT version for testing
VERSION=1.15.0
mvn versions:set -DnewVersion=$VERSION
git add pom.xml
git add */pom.xml
```

#### Update Changelog
Update Changelog using [Github Changelog Generator](https://github.com/github-changelog-generator/github-changelog-generator). Note: Make sure you set `$CHANGELOG_GITHUB_TOKEN` in your `.bash_profile` or use the `--token` flag.
```
# For RELEASE CANDIDATE, set VERSION to future release version.
GITHUB_ORG=NASA-PDS
GITHUB_REPO=validate
github_changelog_generator --future-release v$VERSION --user $GITHUB_ORG --project $GITHUB_REPO --configure-sections '{"improvements":{"prefix":"**Improvements:**","labels":["Epic"]},"defects":{"prefix":"**Defects:**","labels":["bug"]},"deprecations":{"prefix":"**Deprecations:**","labels":["deprecation"]}}' --no-pull-requests --token $GITHUB_TOKEN

git add CHANGELOG.md
```

#### Commit Changes
Commit changes using following template commit message:
```
# For operational release
git commit -m "[RELEASE] Validate v$VERSION"

# Push changes to main
git push -u origin main
```

#### Build and Deploy Software to Maven Central Repo

```
# For operational release
mvn clean site site:stage package deploy -P release

# For release candidate
mvn clean site site:stage package deploy
```

#### Push Tagged Release
```
# For Release Candidate, you may need to delete old SNAPSHOT tag
git push origin :v$VERSION

# Now tag and push
REPO=validate
git tag v${VERSION} -m "[RELEASE] $REPO v$VERSION" -m "See [CHANGELOG](https://github.com/NASA-PDS/$REPO/blob/main/CHANGELOG.md) for more details."
git push --tags

```

#### Deploy Site to Github Pages

From cloned repo:
```
git checkout gh-pages

# Copy the over to version-specific and default sites
rsync -av target/staging/ .

git add .

# For operational release
git commit -m "Deploy v$VERSION docs"

# For release candidate
git commit -m "Deploy v${VERSION}-rc${CANDIDATE_NUM} docs"

git push origin gh-pages
```

#### Update Versions For Development

Update `pom.xml` with the next SNAPSHOT version either manually or using Github Versions Plugin.

For RELEASE CANDIDATE, ignore this step.

```
git checkout main

# For release candidates, skip to push changes to main
VERSION=1.16.0-SNAPSHOT
mvn versions:set -DnewVersion=$VERSION
git add pom.xml
git commit -m "Update version for $VERSION development"

# Push changes to main
git push -u origin main
```

#### Complete Release in Github
Currently the process to create more formal release notes and attach Assets is done manually through the Github UI, but should eventually be automated via script.

*NOTE: Be sure to add the `tar.gz` and `zip` from the `target/` directory to the release assets, and use the CHANGELOG generated above to create the RELEASE NOTES.*


### CI/CD
The template repository comes with our two "standard" CI/CD workflows, `stable-cicd` and `unstable-cicd`. The unstable build runs on any push to `main` (+/- ignoring changes to specific files) and the stable build runs on push of a release branch of the form `release/<release version>`. Both of these make use of our GitHub actions build step, [Roundup](https://github.com/NASA-PDS/roundup-action). The `unstable-cicd` will generate (and constantly update) a SNAPSHOT release. If you haven't done a formal software release you will end up with a `v0.0.0-SNAPSHOT` release (see NASA-PDS/roundup-action#56 for specifics). Additionally, tests are executed on any non-`main` branch push via `branch-cicd`.


## 📃 License

The project is licensed under the [Apache version 2](LICENSE.md) license. Or it isn't. Change this after consulting with your lawyers.

