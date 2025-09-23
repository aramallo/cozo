========================
Functions and operators
========================

Functions can be used to build expressions.

All functions except those that extract the current time and those having names starting with ``rand_`` are deterministic.

------------------------------------
Non-functions
------------------------------------

Functions must take in expressions as arguments, evaluate each argument in turn, 
and then evaluate its implementation to produce a value that can be used in an expression.
We first describe constructs that look like, but are not functions.

These are language constucts that return Horn clauses instead of expressions:

* ``var = expr`` unifies ``expr`` with ``var``. Different from ``expr1 == expr2``.
* ``not clause`` negates a Horn clause ``clause``. Different from ``!expr`` or ``negate(expr)``.
* ``clause1 or clause2`` connects two Horn-clauses by disjunction. Different from ``or(expr1, expr2)``.
* ``clause1 and clause2`` connects two Horn-clauses by conjunction. Different from ``and(expr1, expr2)``.
* ``clause1, clause2`` connects two Horn-clauses by conjunction.

For the last three, ``or`` binds more tightly from ``and``, which in turn binds more tightly than ``,``:
``and`` and ``,`` are identical in every aspect except their binding powers.

These are constructs that return expressions:

* ``if(a, b, c)`` evaluates ``a``, and if the result is ``true``, evaluate ``b`` and returns its value, otherwise evaluate ``c`` and returns its value.
  ``a`` must evaluate to a boolean.
* ``if(a, b)`` same as ``if(a, b, null)``
* ``cond(a1, b1, a2, b2, ...)`` evaluates ``a1``, if the results is ``true``, returns the value of ``b1``, otherwise continue with
  ``a2`` and ``b2``. An even number of arguments must be given and the ``a``s must evaluate to booleans.
  If all ``a``s are ``false``, ``null`` is returned. If you want a catch-all clause at the end,
  put ``true`` as the condition.

------------------------------------
Operators representing functions
------------------------------------

Some functions have equivalent operator forms, which are easier to type and perhaps more familiar. First the binary operators:

* ``a && b`` is the same as ``and(a, b)``
* ``a || b`` is the same as ``or(a, b)``
* ``a ^ b`` is the same as ``pow(a, b)``
* ``a ++ b`` is the same as ``concat(a, b)``
* ``a + b`` is the same as ``add(a, b)``
* ``a - b`` is the same as ``sub(a, b)``
* ``a * b`` is the same as ``mul(a, b)``
* ``a / b`` is the same as ``div(a, b)``
* ``a % b`` is the same as ``mod(a, b)``
* ``a >= b`` is the same as ``ge(a, b)``
* ``a <= b`` is the same as ``le(a, b)``
* ``a > b`` is the same as ``gt(a, b)``
* ``a < b`` is the same as ``lt(a, b)``
* ``a == b`` is the same as ``eq(a, b)``
* ``a != b`` is the same as ``neq(a, b)``
* ``a ~ b`` is the same as ``coalesce(a, b)``
* ``a -> b`` is the same as ``maybe_get(a, b)``

These operators have precedence as follows 
(the earlier rows binds more tightly, and within the same row operators have equal binding power):

* ``->``
* ``~``
* ``^``
* ``*``, ``/``
* ``+``, ``-``, ``++``
* ``%``
* ``==``, ``!=``
* ``>=``, ``<=``, ``>``, ``<``
* ``&&``
* ``||``

With the exception of ``^``, all binary operators are left associative: ``a / b / c`` is the same as
``(a / b) / c``. ``^`` is right associative: ``a ^ b ^ c`` is the same as ``a ^ (b ^ c)``.

And the unary operators are:

* ``-a`` is the same as ``minus(a)``
* ``!a`` is the same as ``negate(a)``

Function applications using parentheses bind the tightest, followed by unary operators, then binary operators.

------------------------
Equality and Comparisons
------------------------

.. module:: Func.EqCmp
    :noindex:
    
.. function:: eq(x, y)

    Equality comparison. The operator form is ``x == y``. The two arguments of the equality can be of different types, in which case the result is ``false``.

.. function:: neq(x, y)

    Inequality comparison. The operator form is ``x != y``. The two arguments of the equality can be of different types, in which case the result is ``true``.

.. function:: gt(x, y)

    Equivalent to ``x > y``

.. function:: ge(x, y)

    Equivalent to ``x >= y``

.. function:: lt(x, y)

    Equivalent to ``x < y``

.. function:: le(x, y)

    Equivalent to ``x <= y``

.. NOTE::

    The four comparison operators can only compare values of the same runtime type. Integers and floats are of the same type ``Number``.

.. function:: max(x, ...)

    Returns the maximum of the arguments. Can only be applied to numbers.

.. function:: min(x, ...)

    Returns the minimum of the arguments. Can only be applied to numbers.

------------------------
Boolean functions
------------------------

.. module:: Func.Bool
    :noindex:
    
.. function:: and(...)

    Variadic conjunction. For binary arguments it is equivalent to ``x && y``.

.. function:: or(...)

    Variadic disjunction. For binary arguments it is equivalent to ``x || y``.

.. function:: negate(x)

    Negation. Equivalent to ``!x``.

.. function:: assert(x, ...)

    Returns ``true`` if ``x`` is ``true``, otherwise will raise an error containing all its arguments as the error message.

------------------------
Mathematics
------------------------

.. module:: Func.Math
    :noindex:
    
.. function:: add(...)

    Variadic addition. The binary version is the same as ``x + y``.

.. function:: sub(x, y)

    Equivalent to ``x - y``.

.. function:: mul(...)

    Variadic multiplication. The binary version is the same as ``x * y``.

.. function:: div(x, y)

    Equivalent to ``x / y``.

.. function:: minus(x)

    Equivalent to ``-x``.

.. function:: pow(x, y)

    Raises ``x`` to the power of ``y``. Equivalent to ``x ^ y``. Always returns floating number.

.. function:: sqrt(x)

    Returns the square root of ``x``.

.. function:: mod(x, y)

    Returns the remainder when ``x`` is divided by ``y``. Arguments can be floats. The returned value has the same sign as ``x``.  Equivalent to ``x % y``.

.. function:: abs(x)

    Returns the absolute value.

.. function:: signum(x)

    Returns ``1``, ``0`` or ``-1``, whichever has the same sign as the argument, e.g. ``signum(to_float('NEG_INFINITY')) == -1``, ``signum(0.0) == 0``, but ``signum(-0.0) == -1``. Returns ``NAN`` when applied to ``NAN``.

.. function:: floor(x)

    Returns the floor of ``x``.

.. function:: ceil(x)

    Returns the ceiling of ``x``.

.. function:: round(x)

    Returns the nearest integer to the argument (represented as Float if the argument itself is a Float). Round halfway cases away from zero. E.g. ``round(0.5) == 1.0``, ``round(-0.5) == -1.0``, ``round(1.4) == 1.0``.

.. function:: exp(x)

    Returns the exponential of the argument, natural base.

.. function:: exp2(x)

    Returns the exponential base 2 of the argument. Always returns a float.

.. function:: ln(x)

    Returns the natual logarithm.

.. function:: log2(x)

    Returns the logarithm base 2.

.. function:: log10(x)

    Returns the logarithm base 10.

.. function:: sin(x)

    The sine trigonometric function.

.. function:: cos(x)

    The cosine trigonometric function.

.. function:: tan(x)

    The tangent trigonometric function.

.. function:: asin(x)

    The inverse sine.

.. function:: acos(x)

    The inverse cosine.

.. function:: atan(x)

    The inverse tangent.

.. function:: atan2(x, y)

    The inverse tangent `atan2 <https://en.wikipedia.org/wiki/Atan2>`_ by passing `x` and `y` separately.

.. function:: sinh(x)

    The hyperbolic sine.

.. function:: cosh(x)

    The hyperbolic cosine.

.. function:: tanh(x)

    The hyperbolic tangent.

.. function:: asinh(x)

    The inverse hyperbolic sine.

.. function:: acosh(x)

    The inverse hyperbolic cosine.

.. function:: atanh(x)

    The inverse hyperbolic tangent.

.. function:: deg_to_rad(x)

    Converts degrees to radians.

.. function:: rad_to_deg(x)

    Converts radians to degrees.

.. function:: haversine(a_lat, a_lon, b_lat, b_lon)

    Computes with the `haversine formula <https://en.wikipedia.org/wiki/Haversine_formula>`_
    the angle measured in radians between two points ``a`` and ``b`` on a sphere
    specified by their latitudes and longitudes. The inputs are in radians.
    You probably want the next function when you are dealing with maps,
    since most maps measure angles in degrees instead of radians.

.. function:: haversine_deg_input(a_lat, a_lon, b_lat, b_lon)

    Same as the previous function, but the inputs are in degrees instead of radians.
    The return value is still in radians.

    If you want the approximate distance measured on the surface of the earth instead of the angle between two points,
    multiply the result by the radius of the earth,
    which is about ``6371`` kilometres, ``3959`` miles, or ``3440`` nautical miles.

    .. NOTE::

        The haversine formula, when applied to the surface of the earth, which is not a perfect sphere, can result in an error of less than one percent.

------------------------
Vector functions
------------------------

Now that mathematical functions that operate on floats can also take vectors as arguments, and apply the operation element-wise.

.. module:: Func.Vector
    :noindex:

.. function:: vec(l, type?)

    Takes a list of numbers and returns a vector.

    Defaults to 32-bit float vectors. If you want to use 64-bit float vectors, pass ``'F64'`` as the second argument.

.. function:: rand_vec(n, type?)

    Returns a vector of ``n`` random numbers between ``0`` and ``1``.

    Defaults to 32-bit float vectors. If you want to use 64-bit float vectors, pass ``'F64'`` as the second argument.

.. function:: l2_normalize(v)

    Takes a vector and returns a vector with the same direction but length ``1``, normalized using L2 norm.

.. function:: l2_dist(u, v)

    Takes two vectors and returns the distance between them, using squared L2 norm: d = sum((ui-vi)^2).

.. function:: ip_dist(u, v)

    Takes two vectors and returns the distance between them, using inner product: d = 1 - sum(ui*vi).

.. function:: cos_dist(u, v)

    Takes two vectors and returns the distance between them, using cosine distance: d = 1 - sum(ui*vi) / (sqrt(sum(ui^2)) * sqrt(sum(vi^2))).

------------------------
Json funcitons
------------------------

.. function:: json(x)

    Converts any value to a Json value. This function is idempotent and never fails.

.. function:: is_json(x)

    Returns ``true`` if the argument is a Json value, ``false`` otherwise.

.. function:: json_object(k1, v1, ...)

    Convert a list of key-value pairs to a Json object.

.. function:: dump_json(x)

    Convert a Json value to its string representation.

.. function:: parse_json(x)

    Parse a string to a Json value.


.. function:: get(json, idx, default?)

    Returns the element at index ``idx`` in the Json ``json``. 
    
    ``idx`` may be a string (for indexing objects), a number (for indexing arrays), or a list of strings and numbers (for indexing deep structures).
    
    Raises an error if the requested element cannot be found, unless ``default`` is specified, in which cast ``default`` is returned.

.. function:: maybe_get(json, idx)

    Returns the element at index ``idx`` in the Json ``json``. Same as ``get(json, idx, null)``. The shorthand is ``json->idx``.


.. function:: set_json_path(json, path, value)

    Set the value at the given path in the given Json value. The path is a list of keys of strings (for indexing objects) or numbers (for indexing arrays). The value is converted to Json if it is not already a Json value.

.. function:: remove_json_path(json, path)

    Remove the value at the given path in the given Json value. The path is a list of keys of strings (for indexing objects) or numbers (for indexing arrays).

.. function:: json_to_scalar(x)

    Convert a Json value to a scalar value if it is a ``null``, boolean, number or string, and returns the argument unchanged otherwise.

.. function:: concat(x, y, ...)

    Concatenate (deep-merge) Json values. It is equivalent to the operator form ``x ++ y ++ ...``

    The concatenation of two Json arrays is the concatenation of the two arrays. The concatenation of two Json objects is the deep-merge of the two objects, meaning that their key-value pairs are combined, with any pairs that appear in both left and right having their values deep-merged. For all other cases, the right value wins.

------------------------
String functions
------------------------

.. module:: Func.String
    :noindex:

.. function:: length(str)

    Returns the number of Unicode characters in the string.

    Can also be applied to a list or a byte array.


    .. WARNING::

        ``length(str)`` does not return the number of bytes of the string representation.
        Also, what is returned depends on the normalization of the string.
        So if such details are important, apply ``unicode_normalize`` before ``length``.


.. function:: concat(x, ...)

    Concatenates strings. Equivalent to ``x ++ y`` in the binary case.

    Can also be applied to lists.

.. function:: str_includes(x, y)

    Returns ``true`` if ``x`` contains the substring ``y``, ``false`` otherwise.

.. function:: lowercase(x)

    Convert to lowercase. Supports Unicode.

.. function:: uppercase(x)

    Converts to uppercase. Supports Unicode.

.. function:: trim(x)

    Removes `whitespace <https://en.wikipedia.org/wiki/Whitespace_character>`_ from both ends of the string.

.. function:: trim_start(x)

    Removes `whitespace <https://en.wikipedia.org/wiki/Whitespace_character>`_ from the start of the string.

.. function:: trim_end(x)

    Removes `whitespace <https://en.wikipedia.org/wiki/Whitespace_character>`_ from the end of the string.

.. function:: starts_with(x, y)

    Tests if ``x`` starts with ``y``.

    .. TIP::

        ``starts_with(var, str)`` is preferred over equivalent (e.g. regex) conditions,
        since the compiler may more easily compile the clause into a range scan.

.. function:: ends_with(x, y)

    tests if ``x``  ends with ``y``.

.. function:: unicode_normalize(str, norm)

    Converts ``str`` to the `normalization <https://en.wikipedia.org/wiki/Unicode_equivalence>`_ specified by ``norm``.
    The valid values of ``norm`` are ``'nfc'``, ``'nfd'``, ``'nfkc'`` and ``'nfkd'``.

.. function:: chars(str)

    Returns Unicode characters of the string as a list of substrings.

.. function:: from_substrings(list)

    Combines the strings in ``list`` into a big string. In a sense, it is the inverse function of ``chars``.

    .. WARNING::

        If you want substring slices, indexing strings, etc., first convert the string to a list with ``chars``,
        do the manipulation on the list, and then recombine with ``from_substring``.

--------------------------
List functions
--------------------------

.. module:: Func.List
    :noindex:

.. function:: list(x, ...)

    Constructs a list from its argument, e.g. ``list(1, 2, 3)``. Equivalent to the literal form ``[1, 2, 3]``.

.. function:: is_in(el, list)

    Tests the membership of an element in a list.

.. function:: first(l)

    Extracts the first element of the list. Returns ``null`` if given an empty list.

.. function:: last(l)

    Extracts the last element of the list. Returns ``null`` if given an empty list.

.. function:: get(l, n, default?)

    Returns the element at index ``n`` in the list ``l``. Raises an error if the access is out of bounds, unless ``default`` is specified, in which cast ``default`` is returned. Indices start with 0.

.. function:: maybe_get(l, n)

    Returns the element at index ``n`` in the list ``l``. Same as ``get(l, n, null)``. The shorthand is ``l->n``.

.. function:: length(list)

    Returns the length of the list.

    Can also be applied to a string or a byte array.

.. function:: slice(l, start, end)

    Returns the slice of list between the index ``start`` (inclusive) and ``end`` (exclusive).
    Negative numbers may be used, which is interpreted as counting from the end of the list.
    E.g. ``slice([1, 2, 3, 4], 1, 3) == [2, 3]``, ``slice([1, 2, 3, 4], 1, -1) == [2, 3]``.

.. function:: concat(x, ...)

    Concatenates lists. The binary case is equivalent to ``x ++ y``.

    Can also be applied to strings.

.. function:: prepend(l, x)

    Prepends ``x`` to ``l``.

.. function:: append(l, x)

    Appends ``x`` to ``l``.

.. function:: reverse(l)

    Reverses the list.

.. function:: sorted(l)

    Sorts the list and returns the sorted copy.

.. function:: chunks(l, n)

    Splits the list ``l`` into chunks of ``n``, e.g. ``chunks([1, 2, 3, 4, 5], 2) == [[1, 2], [3, 4], [5]]``.

.. function:: chunks_exact(l, n)

    Splits the list ``l`` into chunks of ``n``, discarding any trailing elements, e.g. ``chunks([1, 2, 3, 4, 5], 2) == [[1, 2], [3, 4]]``.

.. function:: windows(l, n)

    Splits the list ``l`` into overlapping windows of length ``n``. e.g. ``windows([1, 2, 3, 4, 5], 3) == [[1, 2, 3], [2, 3, 4], [3, 4, 5]]``.

.. function:: union(x, y, ...)

    Computes the set-theoretic union of all the list arguments.

.. function:: intersection(x, y, ...)

    Computes the set-theoretic intersection of all the list arguments.

.. function:: difference(x, y, ...)

    Computes the set-theoretic difference of the first argument with respect to the rest.



----------------
Binary functions
----------------

.. module:: Func.Bin
    :noindex:

.. function:: length(bytes)

    Returns the length of the byte array.

    Can also be applied to a list or a string.

.. function:: bit_and(x, y)

    Calculate the bitwise and. The two bytes must have the same lengths.

.. function:: bit_or(x, y)

    Calculate the bitwise or. The two bytes must have the same lengths.

.. function:: bit_not(x)

    Calculate the bitwise not.

.. function:: bit_xor(x, y)

    Calculate the bitwise xor. The two bytes must have the same lengths.

.. function:: pack_bits([...])

    packs a list of booleans into a byte array; if the list is not divisible by 8, it is padded with ``false``.

.. function:: unpack_bits(x)

    Unpacks a byte array into a list of booleans.

.. function:: encode_base64(b)

    Encodes the byte array ``b`` into the `Base64 <https://en.wikipedia.org/wiki/Base64>`_-encoded string.

    .. NOTE::
        ``encode_base64`` is automatically applied when output to JSON since JSON cannot represent bytes natively.

.. function:: decode_base64(str)

    Tries to decode the ``str`` as a `Base64 <https://en.wikipedia.org/wiki/Base64>`_-encoded byte array.


--------------------------------
Type checking and conversions
--------------------------------

.. module:: Func.Typing
    :noindex:

.. function:: coalesce(x, ...)

    Returns the first non-null value; ``coalesce(x, y)`` is equivalent to ``x ~ y``.

.. function:: to_string(x)
    
    Convert ``x`` to a string: the argument is unchanged if it is already a string, otherwise its JSON string representation will be returned.

.. function:: to_float(x)

    Tries to convert ``x`` to a float. Conversion from numbers always succeeds. Conversion from strings has the following special cases in addition to the usual string representation:

    * ``INF`` is converted to infinity;
    * ``NEG_INF`` is converted to negative infinity;
    * ``NAN`` is converted to NAN (but don't compare NAN by equality, use ``is_nan`` instead);
    * ``PI`` is converted to pi (3.14159...);
    * ``E`` is converted to the base of natural logarithms, or Euler's constant (2.71828...).

    Converts ``null`` and ``false`` to ``0.0``, ``true`` to ``1.0``.

.. function:: to_int(x)

    Converts to an integer. If ``x`` is a validity, extracts the timestamp as an integer.

.. function:: to_unity(x)

    Tries to convert ``x`` to ``0`` or ``1``: ``null``, ``false``, ``0``, ``0.0``, ``""``, ``[]``, and the empty bytes are converted to ``0``,
    and everything else is converted to ``1``.

.. function:: to_bool(x)

    Tries to convert ``x`` to a boolean. The following are converted to ``false``, and everything else is converted to ``true``:

    * ``null``
    * ``false``
    * ``0``, ``0.0``
    * ``""`` (empty string)
    * the empty byte array
    * the nil UUID (all zeros)
    * ``[]`` (the empty list)
    * any validity that is a retraction

.. function:: to_uuid(x)

    Tries to convert ``x`` to a UUID. The input must either be a hyphenated UUID string representation or already a UUID for it to succeed.

.. function:: uuid_timestamp(x)

    Extracts the timestamp from a UUID version 1, as seconds since the UNIX epoch. If the UUID is not of version 1, ``null`` is returned. If ``x`` is not a UUID, an error is raised.

.. function:: is_null(x)

    Checks for ``null``.

.. function:: is_int(x)

    Checks for integers.

.. function:: is_float(x)

    Checks for floats.

.. function:: is_finite(x)

    Returns ``true`` if ``x`` is an integer or a finite float.

.. function:: is_infinite(x)

    Returns ``true`` if ``x`` is infinity or negative infinity.

.. function:: is_nan(x)

    Returns ``true`` if ``x`` is the special float ``NAN``. Returns ``false`` when the argument is not of number type.

.. function:: is_num(x)

    Checks for numbers.

.. function:: is_bytes(x)

    Checks for bytes.

.. function:: is_list(x)

    Checks for lists.

.. function:: is_string(x)

    Checks for strings.

.. function:: is_uuid(x)

    Checks for UUIDs.

-----------------
Random functions
-----------------

.. module:: Func.Rand
    :noindex:

.. function:: rand_float()

    Generates a float in the interval [0, 1], sampled uniformly.

.. function:: rand_bernoulli(p)

    Generates a boolean with probability ``p`` of being ``true``.

.. function:: rand_int(lower, upper)

    Generates an integer within the given bounds, both bounds are inclusive.

.. function:: rand_choose(list)

    Randomly chooses an element from ``list`` and returns it. If the list is empty, it returns ``null``.

.. function:: rand_uuid_v1()

    Generate a random UUID, version 1 (random bits plus timestamp).
    The resolution of the timestamp part is much coarser on WASM targets than the others.

.. function:: rand_uuid_v4()

    Generate a random UUID, version 4 (completely random bits).

.. function:: rand_vec(n, type?)

    Generates a vector of ``n`` random elements. If ``type`` is not given, it defaults to ``F32``.

------------------
Regex functions
------------------

.. module:: Func.Regex
    :noindex:

.. function:: regex_matches(x, reg)

    Tests if ``x`` matches the regular expression ``reg``.

.. function:: regex_replace(x, reg, y)

    Replaces the first occurrence of the pattern ``reg`` in ``x`` with ``y``.

.. function:: regex_replace_all(x, reg, y)

    Replaces all occurrences of the pattern ``reg`` in ``x`` with ``y``.

.. function:: regex_extract(x, reg)

    Extracts all occurrences of the pattern ``reg`` in ``x`` and returns them in a list.

.. function:: regex_extract_first(x, reg)

    Extracts the first occurrence of the pattern ``reg`` in ``x`` and returns it. If none is found, returns ``null``.


^^^^^^^^^^^^^^^^^
Regex syntax
^^^^^^^^^^^^^^^^^

Matching one character::

    .             any character except new line
    \d            digit (\p{Nd})
    \D            not digit
    \pN           One-letter name Unicode character class
    \p{Greek}     Unicode character class (general category or script)
    \PN           Negated one-letter name Unicode character class
    \P{Greek}     negated Unicode character class (general category or script)

Character classes::

    [xyz]         A character class matching either x, y or z (union).
    [^xyz]        A character class matching any character except x, y and z.
    [a-z]         A character class matching any character in range a-z.
    [[:alpha:]]   ASCII character class ([A-Za-z])
    [[:^alpha:]]  Negated ASCII character class ([^A-Za-z])
    [x[^xyz]]     Nested/grouping character class (matching any character except y and z)
    [a-y&&xyz]    Intersection (matching x or y)
    [0-9&&[^4]]   Subtraction using intersection and negation (matching 0-9 except 4)
    [0-9--4]      Direct subtraction (matching 0-9 except 4)
    [a-g~~b-h]    Symmetric difference (matching `a` and `h` only)
    [\[\]]        Escaping in character classes (matching [ or ])

Composites::

    xy    concatenation (x followed by y)
    x|y   alternation (x or y, prefer x)

Repetitions::

    x*        zero or more of x (greedy)
    x+        one or more of x (greedy)
    x?        zero or one of x (greedy)
    x*?       zero or more of x (ungreedy/lazy)
    x+?       one or more of x (ungreedy/lazy)
    x??       zero or one of x (ungreedy/lazy)
    x{n,m}    at least n x and at most m x (greedy)
    x{n,}     at least n x (greedy)
    x{n}      exactly n x
    x{n,m}?   at least n x and at most m x (ungreedy/lazy)
    x{n,}?    at least n x (ungreedy/lazy)
    x{n}?     exactly n x

Empty matches::

    ^     the beginning of the text
    $     the end of the text
    \A    only the beginning of the text
    \z    only the end of the text
    \b    a Unicode word boundary (\w on one side and \W, \A, or \z on the other)
    \B    not a Unicode word boundary


--------------------
Timestamp functions
--------------------

.. function:: now()

    Returns the current timestamp as seconds since the UNIX epoch.
    The resolution is much coarser on WASM targets than the others.

.. function:: format_timestamp(ts, tz?)

    Interpret ``ts`` as seconds since the epoch and format as a string according to `RFC3339 <https://www.rfc-editor.org/rfc/rfc3339>`_.
    If ``ts`` is a validity, its timestamp will be converted to seconds and used.

    If a second string argument is provided, it is interpreted as a `timezone <https://en.wikipedia.org/wiki/Tz_database>`_ and used to format the timestamp.

.. function:: parse_timestamp(str)

    Parse ``str`` into seconds since the epoch according to RFC3339.

.. function:: validity(ts_micro, is_assert?)

    Returns a validity object with the given timestamp in microseconds.
    If ``is_assert`` is ``true``, the validity will be asserted, otherwise it will be assumed. Defaults to ``true``.

------------------------------------
Enhanced Timestamp Functions
------------------------------------

.. module:: Func.TimestampEnhanced
    :noindex:

.. function:: to_local_parts(instant_utc, tz)

    Converts a UTC timestamp to local date/time components in the specified timezone.

    :param instant_utc: UTC timestamp as seconds since epoch (integer or float)
    :param tz: Timezone string (e.g., "America/New_York", "UTC", "Europe/London")
    :returns: JSON object with fields: ``{year, month, day, hour, minute, second, dow, yday}``
              where ``dow`` is day of week (1=Monday...7=Sunday, ISO 8601) and ``yday`` is day of year

.. function:: from_local_parts(year, month, day, hour, minute, second, tz)

    Constructs a UTC timestamp from local date/time components in the specified timezone.

    :param year: Year (integer)
    :param month: Month (1-12)
    :param day: Day of month (1-31)
    :param hour: Hour (0-23)
    :param minute: Minute (0-59)
    :param second: Second (0-59)
    :param tz: Timezone string
    :returns: UTC timestamp as integer (seconds since epoch)

.. function:: year(instant, tz)

    Extracts the year from a timestamp in the specified timezone.

    :param instant: UTC timestamp (integer or float)
    :param tz: Timezone string
    :returns: Year as integer

.. function:: month(instant, tz)

    Extracts the month from a timestamp in the specified timezone.

    :param instant: UTC timestamp (integer or float)
    :param tz: Timezone string
    :returns: Month as integer (1-12)

.. function:: day(instant, tz)

    Extracts the day of month from a timestamp in the specified timezone.

    :param instant: UTC timestamp (integer or float)
    :param tz: Timezone string
    :returns: Day as integer (1-31)

.. function:: dow(instant, tz)

    Extracts the day of week from a timestamp in the specified timezone.

    :param instant: UTC timestamp (integer or float)
    :param tz: Timezone string
    :returns: Day of week as integer (1=Monday...7=Sunday, ISO 8601)

.. function:: hour(instant, tz)

    Extracts the hour from a timestamp in the specified timezone.

    :param instant: UTC timestamp (integer or float)
    :param tz: Timezone string
    :returns: Hour as integer (0-23)

.. function:: minute(instant, tz)

    Extracts the minute from a timestamp in the specified timezone.

    :param instant: UTC timestamp (integer or float)
    :param tz: Timezone string
    :returns: Minute as integer (0-59)

.. function:: days_in_month(year, month, tz)

    Returns the number of days in the specified month and year.

    :param year: Year (integer)
    :param month: Month (1-12)
    :param tz: Timezone string (used for calendar calculations)
    :returns: Number of days in the month (28-31)

.. function:: start_of_day_local(instant, tz)

    Returns the UTC timestamp for the start of the day (00:00) in the local timezone.

    :param instant: UTC timestamp (integer or float)
    :param tz: Timezone string
    :returns: UTC timestamp for 00:00 local time as integer

------------------------
Interval Functions
------------------------

.. module:: Func.Interval
    :noindex:

Intervals are represented as lists of two integers ``[start, end]`` where ``start < end``.

.. function:: interval(s, e)

    Creates an interval from start and end timestamps.

    :param s: Start timestamp (integer)
    :param e: End timestamp (integer, must be > s)
    :returns: Interval as list ``[s, e]``

.. function:: interval_len(iv)

    Returns the length (duration) of an interval.

    :param iv: Interval as list ``[start, end]``
    :returns: Duration as integer (end - start)

.. function:: interval_intersects(a, b)

    Tests if two intervals intersect (have any overlap).

    :param a: First interval ``[start, end]``
    :param b: Second interval ``[start, end]``
    :returns: Boolean, ``true`` if intervals intersect

.. function:: interval_overlap(a, b)

    Returns the overlapping portion of two intervals.

    :param a: First interval ``[start, end]``
    :param b: Second interval ``[start, end]``
    :returns: Overlapping interval ``[start, end]`` or ``null`` if no overlap

.. function:: interval_union(a, b)

    Returns the union of two intervals as a normalized list.

    :param a: First interval ``[start, end]``
    :param b: Second interval ``[start, end]``
    :returns: List of intervals representing the union (normalized)

.. function:: interval_minus(a, b)

    Subtracts interval ``b`` from interval ``a``.

    :param a: Source interval ``[start, end]``
    :param b: Interval to subtract ``[start, end]``
    :returns: List of 0-2 intervals representing ``a`` minus ``b``

.. function:: interval_adjacent(a, b)

    Tests if two intervals are adjacent (one ends where the other begins).

    :param a: First interval ``[start, end]``
    :param b: Second interval ``[start, end]``
    :returns: Boolean, ``true`` if intervals are adjacent

.. function:: interval_merge_adjacent(sorted_intervals)

    Merges adjacent and overlapping intervals in a sorted list.

    :param sorted_intervals: List of intervals sorted by start time
    :returns: List of merged intervals (coalesced)

.. function:: interval_shift(iv, d)

    Shifts an interval by a given duration.

    :param iv: Interval ``[start, end]``
    :param d: Duration to shift (integer, can be negative)
    :returns: Shifted interval ``[start+d, end+d]``

.. function:: interval_contains(a, t)

    Tests if an interval contains a specific timestamp.

    :param a: Interval ``[start, end]``
    :param t: Timestamp (integer)
    :returns: Boolean, ``true`` if ``start <= t < end``

.. function:: interval_contains_interval(a, b)

    Tests if interval ``a`` completely contains interval ``b``.

    :param a: Container interval ``[start, end]``
    :param b: Contained interval ``[start, end]``
    :returns: Boolean, ``true`` if ``a`` contains ``b``

------------------------------------
Allen Interval Algebra Functions
------------------------------------

.. module:: Func.Allen
    :noindex:

The Allen interval algebra defines 13 basic relationships between intervals.
For intervals ``a=[as,ae]`` and ``b=[bs,be]``:

.. function:: allen_before(a, b)

    Tests if interval ``a`` is completely before interval ``b``.

    :param a: First interval ``[start, end]``
    :param b: Second interval ``[start, end]``
    :returns: Boolean, ``true`` if ``ae < bs``

.. function:: allen_meets(a, b)

    Tests if interval ``a`` meets interval ``b`` (``a`` ends where ``b`` begins).

    :param a: First interval ``[start, end]``
    :param b: Second interval ``[start, end]``
    :returns: Boolean, ``true`` if ``ae == bs``

.. function:: allen_overlaps(a, b)

    Tests if interval ``a`` overlaps the start of interval ``b``.

    :param a: First interval ``[start, end]``
    :param b: Second interval ``[start, end]``
    :returns: Boolean, ``true`` if ``as < bs < ae < be``

.. function:: allen_starts(a, b)

    Tests if interval ``a`` starts interval ``b`` (same start, ``a`` ends first).

    :param a: First interval ``[start, end]``
    :param b: Second interval ``[start, end]``
    :returns: Boolean, ``true`` if ``as == bs && ae < be``

.. function:: allen_during(a, b)

    Tests if interval ``a`` is completely during interval ``b``.

    :param a: First interval ``[start, end]``
    :param b: Second interval ``[start, end]``
    :returns: Boolean, ``true`` if ``bs < as < ae < be``

.. function:: allen_finishes(a, b)

    Tests if interval ``a`` finishes interval ``b`` (same end, ``a`` starts later).

    :param a: First interval ``[start, end]``
    :param b: Second interval ``[start, end]``
    :returns: Boolean, ``true`` if ``as > bs && ae == be``

.. function:: allen_equals(a, b)

    Tests if two intervals are exactly equal.

    :param a: First interval ``[start, end]``
    :param b: Second interval ``[start, end]``
    :returns: Boolean, ``true`` if ``as == bs && ae == be``

.. function:: allen_after(a, b)

    Tests if interval ``a`` is completely after interval ``b`` (inverse of ``allen_before``).

    :param a: First interval ``[start, end]``
    :param b: Second interval ``[start, end]``
    :returns: Boolean, ``true`` if ``allen_before(b, a)``

.. function:: allen_met_by(a, b)

    Tests if interval ``a`` is met by interval ``b`` (inverse of ``allen_meets``).

    :param a: First interval ``[start, end]``
    :param b: Second interval ``[start, end]``
    :returns: Boolean, ``true`` if ``allen_meets(b, a)``

.. function:: allen_overlapped_by(a, b)

    Tests if interval ``a`` is overlapped by interval ``b`` (inverse of ``allen_overlaps``).

    :param a: First interval ``[start, end]``
    :param b: Second interval ``[start, end]``
    :returns: Boolean, ``true`` if ``allen_overlaps(b, a)``

.. function:: allen_started_by(a, b)

    Tests if interval ``a`` is started by interval ``b`` (inverse of ``allen_starts``).

    :param a: First interval ``[start, end]``
    :param b: Second interval ``[start, end]``
    :returns: Boolean, ``true`` if ``allen_starts(b, a)``

.. function:: allen_contains(a, b)

    Tests if interval ``a`` contains interval ``b`` (inverse of ``allen_during``).

    :param a: First interval ``[start, end]``
    :param b: Second interval ``[start, end]``
    :returns: Boolean, ``true`` if ``allen_during(b, a)``

.. function:: allen_finished_by(a, b)

    Tests if interval ``a`` is finished by interval ``b`` (inverse of ``allen_finishes``).

    :param a: First interval ``[start, end]``
    :param b: Second interval ``[start, end]``
    :returns: Boolean, ``true`` if ``allen_finishes(b, a)``

------------------------
Advanced Utility Functions
------------------------

.. module:: Func.AdvancedUtil
    :noindex:

.. function:: expand_weekly_days(h0, h1, by_wday, tz, start_min, end_min)

    Expands weekly recurring patterns into UTC intervals.

    :param h0: Start UTC timestamp (integer)
    :param h1: End UTC timestamp (integer)
    :param by_wday: Set of weekdays (1=Monday...7=Sunday)
    :param tz: Timezone string
    :param start_min: Start minute offset within each day
    :param end_min: End minute offset within each day
    :returns: List of UTC intervals for matching days

.. function:: expand_monthly_setpos(h0, h1, by_wday, by_setpos, tz, start_min, end_min)

    Expands monthly recurring patterns using setpos rules.

    :param h0: Start UTC timestamp (integer)
    :param h1: End UTC timestamp (integer)
    :param by_wday: Set of weekdays (1=Monday...7=Sunday)
    :param by_setpos: Set of position numbers (±1..±5, where negative counts from end)
    :param tz: Timezone string
    :param start_min: Start minute offset within each day
    :param end_min: End minute offset within each day
    :returns: List of UTC intervals for matching occurrences

.. function:: normalize_intervals(intvs)

    Normalizes a list of intervals by sorting and merging overlaps/adjacent intervals.

    :param intvs: List of intervals ``[[start, end], ...]``
    :returns: Normalized list of non-overlapping intervals

.. function:: intervals_minus(intvs, subs)

    Subtracts multiple intervals from a list of intervals.

    :param intvs: List of source intervals
    :param subs: List of intervals to subtract
    :returns: List of intervals representing the difference

.. function:: nth_weekday_of_month(year, month, weekday, n, tz)

    Finds the nth occurrence of a weekday in a given month.

    :param year: Year (integer)
    :param month: Month (1-12)
    :param weekday: Day of week (1=Monday...7=Sunday)
    :param n: Occurrence number (1..5 from start, -1..-5 from end)
    :param tz: Timezone string
    :returns: JSON object ``{year, month, day}`` or ``null`` if not found

.. function:: local_minutes_to_parts(base_local_midnight_utc, minutes, tz)

    Converts minutes offset to date/time parts using DST-aware calculation.

    :param base_local_midnight_utc: UTC timestamp for local midnight (integer)
    :param minutes: Minutes offset from midnight (integer)
    :param tz: Timezone string
    :returns: JSON object ``{year, month, day, hour, minute}``

.. function:: parts_to_instant_utc(parts, tz)

    Converts date/time parts to UTC timestamp.

    :param parts: JSON object ``{year, month, day, hour, minute}``
    :param tz: Timezone string
    :returns: UTC timestamp as integer

------------------------
Bucket Functions
------------------------

.. module:: Func.Bucket
    :noindex:

Bucket functions provide time-based bucketing for data aggregation and time series analysis.

.. function:: bucket_of(t, period, epoch0)

    Returns the bucket number containing the given timestamp.

    :param t: Timestamp (integer)
    :param period: Bucket period/duration (integer)
    :param epoch0: Epoch start time (integer)
    :returns: Bucket number as integer

.. function:: bucket_start(k, period, epoch0)

    Returns the start timestamp of the given bucket.

    :param k: Bucket number (integer)
    :param period: Bucket period/duration (integer)
    :param epoch0: Epoch start time (integer)
    :returns: Start timestamp of bucket as integer

.. function:: ceil_to_bucket(t, period, epoch0)

    Rounds timestamp up to the start of the next bucket.

    :param t: Timestamp (integer)
    :param period: Bucket period/duration (integer)
    :param epoch0: Epoch start time (integer)
    :returns: Start timestamp of next bucket as integer

.. function:: floor_to_bucket(t, period, epoch0)

    Rounds timestamp down to the start of the current bucket.

    :param t: Timestamp (integer)
    :param period: Bucket period/duration (integer)
    :param epoch0: Epoch start time (integer)
    :returns: Start timestamp of current bucket as integer

.. function:: duration_in_buckets(d, period)

    Converts a duration to number of buckets.

    :param d: Duration (integer)
    :param period: Bucket period/duration (integer)
    :returns: Number of buckets as integer