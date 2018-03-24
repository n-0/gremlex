readonly VERSION_FILE="VERSION"
readonly TOP_PID=$$
readonly BRANCH="good-commit-branch"
trap "exit 1" TERM

exit_script() {
    kill -s TERM $TOP_PID
}

run_tests() {
    docker-compose -f docker-compose/docker-compose.yml build && \
        docker-compose -f docker-compose/docker-compose.yml run gremlex && \
        docker-compose -f docker-compose/docker-compose.yml down
    if [ "$?" -ne 0 ]
    then
        echo "Error: Failed to run tests"
        exit_script
    fi
}

lint() {
    commitlint --from master
    if [ "$?" -ne 0 ]
    then
        echo "Error: Commitlint failed"
        exit_script
    fi
}

get_updated_version() {
    major=$(cut -d '.' -f 1 ${VERSION_FILE})
    minor=$(cut -d '.' -f 2 ${VERSION_FILE} | xargs -I '{}' expr '{}' + 1 )
    patch=$(cut -d '.' -f 3 ${VERSION_FILE})
    echo "${major}.${minor}.${patch}"
}

update_version() {
    echo $1 > $VERSION_FILE
    npm version --no-git-tag-version $1
}

update_changelog() {
    # Update changelog in place
    conventional-changelog -i CHANGELOG.md -s
}

commit_and_tag() {
    git checkout $BRANCH
    git add $VERSION_FILE
    git add CHANGELOG.md
    git commit -m "$1"
    git tag "$1"
    if [ "$?" -ne 0 ]
    then
        echo "Failed to commit or tag new version"
        exit_script
    fi
}

push_master() {
    git push origin $BRANCH
    if [ "$?" -ne 0 ]
    then
        echo "Failed to push new version commit to master"
        exit_script
    fi
}

push_tag() {
    git push origin $1
    if [ "$?" -ne 0 ]
    then
        echo "Failed to push tag to master"
        exit_script
    fi
}

main() {
    # Steps: test, lint, commit, tag, upload
    run_tests
    version=$(get_updated_version)
    update_version $version
    update_changelog
    commit_and_tag $version
    # push_master
    # push_tag $version
    exit 0
}

main
