# Clone the repository inside your projects folder
cd <directory/to/make/it>
git clone https://github.com/yellow-solar/newfeefee-v1.git

# You can also close a single branch name (if not master) via
git clone -b opencv-2.4 --single-branch https://github.com/Itseez/opencv.git

# Get current status of branch statuses
git status

# Create new branch and activate it
git checkout -b <new-branch-name>

# You can switch back to another branch using checkout as well
git checkout -b <old-branch-name>

# Add a file to the stage environment
git add <filename>

# Commit to the staging environment
git commit -m "short message about the commit"

# NOTE: you can add and commit straight to the master, but highly recommend checking out to a form and then PULL/MERGE -- see below

# Push changes from a branch to the master?
git push origin yourbranchname

# Manage the pull request and merge on the GitHub website. Approve, and delete old branch

# After that you can view the log - press q to exit log
git log

# Check the current branch - make sure you're aware of what branch you're working in
git branch

#ON AWS WHEN YOU WANT TO OVERRIDE
git fetch # fetch current branch
git reset --hard origin/master



# Git commands
https://guides.github.com/introduction/git-handbook/
# more detail
https://git-scm.com/docs

#
