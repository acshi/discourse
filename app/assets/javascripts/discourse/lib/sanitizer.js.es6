const _validTags = {};
const _validClasses = {};
const _validIframes = [];
let _decoratedCaja = false;

function validateAttribute(tagName, attribName, value) {
  var tag = _validTags[tagName];

  // Handle classes
  if (attribName === "class") {
    if (_validClasses[value]) { return value; }
  }

  if (attribName.indexOf('data-') === 0) {
    // data-* catch-all validators
    if (tag && tag['data-*'] && !tag[attribName]) {
      var permitted = tag['data-*'];
      if (permitted && (
            permitted.indexOf(value) !== -1 ||
            permitted.indexOf('*') !== -1 ||
            ((permitted instanceof RegExp) && permitted.test(value)))
        ) { return value; }
    }
  }

  if (tag) {
    var attrs = tag[attribName];
    if (attrs && (attrs.indexOf(value) !== -1 ||
                  attrs.indexOf('*') !== -1) ||
                  _.any(attrs, function(r) { return (r instanceof RegExp) && r.test(value); })
        ) { return value; }
  }
}

function anchorRegexp(regex) {
  if (/^\^.*\$$/.test(regex.source)) {
    return regex; // already anchored
  }

  var flags = "";
  if (regex.global) {
    if (typeof console !== 'undefined') {
      console.warn("attribute validation regex should not be global");
    }
  }

  if (regex.ignoreCase) { flags += "i"; }
  if (regex.multiline) { flags += "m"; }
  if (regex.sticky) { throw "Invalid attribute validation regex - cannot be sticky"; }

  return new RegExp("^" + regex.source + "$", flags);
}

export function urlAllowed(uri, effect, ltype, hints) {
  var url = typeof(uri) === "string" ? uri : uri.toString();

  // escape single quotes
  url = url.replace(/'/g, "%27");

  // whitelist some iframe only
  if (hints && hints.XML_TAG === "iframe" && hints.XML_ATTR === "src") {
    for (var i = 0, length = _validIframes.length; i < length; i++) {
      if(_validIframes[i].test(url)) { return url; }
    }
    return;
  }

  // absolute urls
  if(/^(https?:)?\/\/[\w\.\-]+/i.test(url)) { return url; }
  // relative urls
  if(/^\/[\w\.\-]+/i.test(url)) { return url; }
  // anchors
  if(/^#[\w\.\-]+/i.test(url)) { return url; }
  // mailtos
  if(/^mailto:[\w\.\-@]+/i.test(url)) { return url; }
};

export function sanitize(text) {
  if (!window.html_sanitize || !text) return "";

  // Allow things like <3 and <_<
  text = text.replace(/<([^A-Za-z\/\!]|$)/g, "&lt;$1");

  // The first time, let's add some more whitelisted tags
  if (!_decoratedCaja) {

    // Add anything whitelisted to the list of elements if it's not in there already.
    var elements = window.html4.ELEMENTS;
    Object.keys(_validTags).forEach(function(t) {
      if (!elements[t]) {
        elements[t] = 0;
      }
    });

    _decoratedCaja = true;
  }

  return window.html_sanitize(text, urlAllowed, validateAttribute);
};

export function whiteListTag(tagName, attribName, value) {
  if (value instanceof RegExp) {
    value = anchorRegexp(value);
  }
  _validTags[tagName] = _validTags[tagName] || {};
  _validTags[tagName][attribName] = _validTags[tagName][attribName] || [];
  _validTags[tagName][attribName].push(value || '*');
}

export function whiteListClass() {
  var args = Array.prototype.slice.call(arguments);
  args.forEach(function (a) { _validClasses[a] = true; });
}

export function whiteListIframe(regexp) {
  _validIframes.push(regexp);
}

whiteListTag('a', 'class', 'attachment');
whiteListTag('a', 'class', 'onebox');
whiteListTag('a', 'class', 'mention');
whiteListTag('a', 'class', 'mention-group');
whiteListTag('a', 'class', 'hashtag');

whiteListTag('a', 'target', '_blank');
whiteListTag('a', 'rel', 'nofollow');
whiteListTag('a', 'data-bbcode');
whiteListTag('a', 'name');

whiteListTag('img', 'src', /^data:image.*$/i);

whiteListTag('div', 'class', 'title');
whiteListTag('div', 'class', 'quote-controls');

whiteListTag('span', 'class', 'mention');
whiteListTag('span', 'class', 'hashtag');
whiteListTag('aside', 'class', 'quote');
whiteListTag('aside', 'data-*');

whiteListTag('span', 'bbcode-b');
whiteListTag('span', 'bbcode-i');
whiteListTag('span', 'bbcode-u');
whiteListTag('span', 'bbcode-s');

// used for pinned topics
whiteListTag('span', 'class', 'excerpt');
whiteListIframe(/^(https?:)?\/\/www\.google\.com\/maps\/embed\?.+/i);
whiteListIframe(/^(https?:)?\/\/www\.openstreetmap\.org\/export\/embed.html\?.+/i);

