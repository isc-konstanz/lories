# Contributing to the Code

Encouraging more people to help develop loris is essential to our success.
Therefore, we want to make it easy and rewarding for you to contribute.

There is a lot of material in this section, aimed at a variety of contributors from novice to expert.
Don't worry if you don't (yet) understand parts of it.


## How to contribute new code

### The basics

Contributors to loris use GitHub's pull requests to add/modify its source code.
The GitHub pull request process can be intimidating for new users,
but you'll find that it becomes straightforward once you use it a few times.
Please let us know if you get stuck at any point in the process.
Here's an outline of the process:

- Create a [GitHub issue](https://github.com/isc-konstanz/loris/issues) and get initial feedback
  from users and maintainers.  
  If the issue is a bug report, please take a look at how to [report bugs](bugs).
- Obtain the latest version of loris: Fork the loris project to your GitHub account,
  ``git clone`` your fork to your computer.
- Make some or all of your changes/additions and ``git commit`` them to your local repository.
- Share your changes with us via a pull request: ``git push`` your local changes to your GitHub fork,
  then go to GitHub make a pull request.

The Pandas project maintains an excellent [Contributing Page](http://pandas.pydata.org/pandas-docs/stable/contributing.html)
that goes into detail on each of these steps and also provides great
[Contribution Guidelines](https://pandas.pydata.org/pandas-docs/stable/development/contributing_codebase.html).
Also see GitHub's [Set Up Git](https://help.github.com/articles/set-up-git/)
and [Using Pull Requests](https://help.github.com/articles/using-pull-requests/).

We strongly recommend using virtual environments for development.
Virtual environments make it trivial to switch between different versions of software.
This [Astropy Guide](http://astropy.readthedocs.org/en/latest/development/workflow/virtual_pythons.html)
is a good reference for virtual environments. If this is your first pull request,
don't worry about using a virtual environment.

You must include documentation for any new or improved code. We can provide help and advice on this after
you start the pull request. See the documentation section below


### When should I submit a pull request?

The short answer: anytime.

The long answer: it depends. If in doubt, go ahead and submit.
You do not need to make all of your changes before creating a pull request.
Your pull requests will automatically be updated when you commit new changes and push them to GitHub.

There are pros and cons to submitting incomplete pull-requests. On the plus side,
it gives everybody an easy way to comment on the code and can make the process more efficient.
On the minus side, it's easy for an incomplete pull request to grow into a multi-month saga that leaves
everyone unhappy. If you submit an incomplete pull request, please be very clear about what you would
like feedback on and what we should ignore.
Alternatives to incomplete pull requests include creating a [gist](https://gist.github.com) or experimental
branch and linking to it in the corresponding issue.

The best way to ensure that a pull request will be reviewed and merged in a timely manner is to:

- Start by creating an issue. The issue should be well-defined and actionable.
- Ask the [maintainers](https://github.com/orgs/pvlib/people) to tag the issue with the appropriate milestone.
- Make a limited-scope pull request. It can be a lot of work to check all of the boxes in the recommended
  [contribution guidelines](https://pandas.pydata.org/pandas-docs/stable/development/contributing_codebase.html)
  especially for pull requests with a lot of new primary code.
- Tag loris community members or ``@pvlib`` when the pull request is ready for review.

(code-style)=
## Code Style

Loris generally follows the the [Python PEP8](https://peps.python.org/pep-0008/) code style.
Maximum line length for code is 120 characters.  
Additionally, the use of [Black](https://black.readthedocs.io/en/stable/) is aspired to ensure a consistent code style across the code base.

Please see the [Documentation section](#documentation) for information specific to documentation style.

Remove any ``logging`` calls and ``print`` statements that you added
during development. ``warning`` is ok.

(documentation)=
## Documentation

```{warning}
The use of descriptive docstrings for all loris functions, classes and class methods is mandatory.
Loris has adopted the [Google Docstring Style](https://google.github.io/styleguide/pyguide.html).
```

Writing code that is easy to understand is a key principle of loris.
This is why it is not enough to write code with descriptive variable names and comments.
All functions, classes and class methods must be documented with a [docstring](https://en.wikipedia.org/wiki/docstring),
similar to this [google style example](https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html),
which is rendered using the [Sphinx Napoleon extension](https://www.sphinx-doc.org/en/master/usage/extensions/napoleon.html).
These docstrings are used to automatically generate the [loris documentation](https://loris.readthedocs.io/).

As such, they should form the basis of the documentation of the loris code base.


### Building the documentation

Building the documentation locally is useful for testing out changes to the documentation's source code
without having to repeatedly update a pull request and have Read the Docs build it for you.
Building the docs locally requires installing loris as an editable library, including the ``doc`` dependencies
specified in the.
An easy way to do this is with:

    pip install loris[doc]    # on Mac:  pip install "loris[doc]"

Once the ``doc`` dependencies are installed, navigate to ``/doc`` and execute:

    make html

Be sure to skim through the output of this command because Sphinx might emit helpful warnings about problems
with the documentation source code. If the build succeeds, it will make a new directory ``doc/_build`` with the
documentation's homepage located at ``_build/html/index.html``.
This file can be opened with a web browser to view the local version like any other website.
Other output formats are available. Run ``make help`` for more information.

Note that Windows users need not have the ``make`` utility installed as loris includes a ``make.bat`` batch file
that emulates its interface.
