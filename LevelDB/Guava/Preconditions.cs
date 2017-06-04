using System;
using System.Text;

namespace LevelDB.Guava
{
    /// <summary>
    /// Porting class from Google Guava
    /// </summary>
    public class Preconditions
    {
        public static T CheckNotNull<T>(T obj)
        {
            if (obj == null)
            {
                throw new NullReferenceException();
            }
            return obj;
        }

        public static T CheckNotNull<T>(T obj, string message)
        {
            if (obj == null)
            {
                throw new NullReferenceException(message);
            }
            return obj;
        }

        /// <summary>
        /// Ensures the truth of an expression involving one or more parameters to the calling method.
        /// </summary>
        /// <param name="expression">A boolean expression</param>
        /// <exception cref="ArgumentException">If expression is false</exception>
        public static void CheckArgument(bool expression)
        {
            if (!expression)
            {
                throw new ArgumentException();
            }
        }

        /// <summary>
        /// Ensures the truth of an expression involving one or more parameters to the calling method.
        /// </summary>
        /// <param name="expression">A boolean expression</param>
        /// <param name="errorMessage">The exception message to use if the check fails; will be converted to a string
        /// using <code cref="Convert.ToString(object)" /></param>
        /// <exception cref="ArgumentException">If expression is false</exception>
        public static void CheckArgument(bool expression, object errorMessage)
        {
            if (!expression)
            {
                throw new ArgumentException(Convert.ToString(errorMessage));
            }
        }

        /// <summary>
        /// Ensures the truth of an expression involving one or more parameters to the calling method.
        /// </summary>
        /// <param name="expression">A boolean expression</param>
        /// <param name="errorMessageTemplate">A template for the exception message should the check fail.
        /// The message is formed by replacing each %s placeholder in the template with an argument.
        /// These are matched by position - the first %s gets errorMessageArgs[0], etc. Unmatched arguments will be
        /// appended to the formatted message in square braces. Unmatched placeholders will be left as-is.</param>
        /// <param name="errorMessageArgs">The arguments to be substituted into the message template.
        /// Arguments are converted to strings using Convert.ToString(object).</param>
        /// <exception cref="ArgumentException">If expression is false</exception>
        public static void CheckArgument(bool expression, string errorMessageTemplate, params object[] errorMessageArgs)
        {
            if (!expression)
            {
                throw new ArgumentException(Format(errorMessageTemplate, errorMessageArgs));
            }
        }

        /// <summary>
        /// Ensures that index specifies a valid position in an array, list or string of size size. A position index may range from zero to size, inclusive.
        /// </summary>
        /// <param name="index">A user-supplied index identifying a position in an array, list or string</param>
        /// <param name="size">The size of that array, list or string</param>
        /// <returns>The value of index</returns>
        /// <exception cref="IndexOutOfRangeException">If index is negative or is greater than size</exception>
        /// <exception cref="ArgumentException">If size is negative</exception>
        public static int CheckPositionIndex(int index, int size)
        {
            return CheckPositionIndex(index, size, nameof(index));
        }

        /// <summary>
        /// Ensures that index specifies a valid position in an array, list or string of size size. A position index may range from zero to size, inclusive.
        /// </summary>
        /// <param name="index">A user-supplied index identifying a position in an array, list or string</param>
        /// <param name="size">The size of that array, list or string</param>
        /// <param name="desc">The text to use to describe this index in an error message</param>
        /// <returns>The value of index</returns>
        /// <exception cref="IndexOutOfRangeException">If index is negative or is greater than size</exception>
        /// <exception cref="ArgumentException">If size is negative</exception>
        public static int CheckPositionIndex(int index, int size, string desc)
        {
            if (index < 0 || index > size)
            {
                throw new IndexOutOfRangeException(BadPositionIndex(index, size, desc));
            }
            return index;
        }

        /// <summary>
        /// Ensures that start and end specify a valid positions in an array, list or string of size size, and are in order. A position index may range from zero to size, inclusive.
        /// </summary>
        /// <param name="start">A user-supplied index identifying a starting position in an array, list or string</param>
        /// <param name="end">A user-supplied index identifying a ending position in an array, list or string</param>
        /// <param name="size">The size of that array, list or string</param>
        /// <exception cref="IndexOutOfRangeException">If either index is negative or is greater than size, or if end is less than start</exception>
        /// <exception cref="ArgumentException">If size is negative</exception>
        public static void CheckPositionIndexes(int start, int end, int size)
        {
            if (start < 0 || end < start || end > size)
            {
                throw new IndexOutOfRangeException(BadPositionIndexes(start, end, size));
            }
        }

        /// <summary>
        /// Ensures the truth of an expression involving the state of the calling instance, but not involving any
        /// parameters to the calling method.
        /// </summary>
        /// <param name="expression">A boolean expression</param>
        /// <exception cref="InvalidOperationException">If <code>expression</code> is false</exception>
        public static void CheckState(bool expression)
        {
            if (!expression)
            {
                throw new InvalidOperationException();
            }
        }

        /// <summary>
        /// Ensures the truth of an expression involving the state of the calling instance, but not involving any
        /// parameters to the calling method.
        /// </summary>
        /// <param name="expression">A boolean expression</param>
        /// <param name="errorMessageTemplate">A template for the exception message should the check fail. The
        /// message is formed by replacing each <code>%s</code> placeholder in the template with an
        /// argument. These are matched by position - the first <code>%s</code> gets <code>errorMessageArgs[0]</code>, etc.
        /// Unmatched arguments will be appended to the formatted message in square braces. Unmatched placeholders will
        /// be left as-is.</param>
        /// <param name="errorMessageArgs">the arguments to be substituted into the message template. Arguments
        /// are converted to strings using <code cref="Convert.ToString(object)"/>.</param>
        /// <exception cref="InvalidOperationException">If <code>expression</code> is false</exception>
        public static void CheckState(bool expression, string errorMessageTemplate, params object[] errorMessageArgs)
        {
            if (!expression)
            {
                throw new InvalidOperationException(Format(errorMessageTemplate, errorMessageArgs));
            }
        }

        /// <summary>
        /// Ensures the truth of an expression involving the state of the calling instance, but not involving any
        /// parameters to the calling method.
        /// </summary>
        /// <param name="expression">A boolean expression</param>
        /// <param name="errorMessage">the exception message to use if the check fails; will be converted to a
        /// string using <code cref="Convert.ToString(object)" /></param>
        /// <exception cref="InvalidOperationException">If <code>expression</code> is false</exception>
        public static void CheckState(bool expression, object errorMessage)
        {
            if (!expression)
            {
                throw new InvalidOperationException(Convert.ToString(errorMessage));
            }
        }

        private static string BadPositionIndexes(int start, int end, int size)
        {
            if (start < 0 || start > size)
            {
                return BadPositionIndex(start, size, "start index");
            }
            if (end < 0 || end > size)
            {
                return BadPositionIndex(end, size, "end index");
            }
            // end < start
            return string.Format("end index ({0}) must not be less than start index ({1})", end, start);
        }

        private static string BadPositionIndex(int index, int size, string desc)
        {
            if (index < 0)
            {
                return string.Format("{0} ({1}) must not be negative", desc, index);
            }
            if (size < 0)
            {
                throw new ArgumentException("negative size: " + size);
            }
            // index > size
            return string.Format("{0} ({1}) must not be greater than size ({2})", desc, index, size);
        }

        /// <summary>
        /// Substitutes each %s in template with an argument. These are matched by position: the first %s gets args[0], etc.
        ///  If there are more arguments than placeholders, the unmatched arguments will be appended to the end of
        /// the formatted message in square braces.
        /// </summary>
        /// <param name="template">A non-null string containing 0 or more %s placeholders.</param>
        /// <param name="args">The arguments to be substituted into the message template.
        /// Arguments are converted to strings using <code cref="Convert.ToString(object)"/>. Arguments can be null.</param>
        /// <returns></returns>
        private static string Format(string template, params object[] args)
        {
            template = Convert.ToString(template); // null -> "null"

            // start substituting the arguments into the '%s' placeholders
            var builder = new StringBuilder(template.Length + 16 * args.Length);
            var templateStart = 0;
            var i = 0;
            while (i < args.Length)
            {
                var placeholderStart = template.IndexOf("%s", templateStart, StringComparison.Ordinal);
                if (placeholderStart == -1)
                {
                    break;
                }
                builder.Append(template.Substring(templateStart, placeholderStart));
                builder.Append(args[i++]);
                templateStart = placeholderStart + 2;
            }
            builder.Append(template.Substring(templateStart));

            // if we run out of placeholders, append the extra args in square braces
            if (i >= args.Length) return builder.ToString();
            builder.Append(" [");
            builder.Append(args[i++]);
            while (i < args.Length)
            {
                builder.Append(", ");
                builder.Append(args[i++]);
            }
            builder.Append(']');

            return builder.ToString();
        }
    }
}