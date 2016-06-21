import xss from 'pretty-text/xss';

const _validIframes = [];

function attr(name, value) {
  return `${name}="${xss.escapeAttrValue(value)}"`;
}

export function hrefAllowed(href) {
  // escape single quotes
  href = href.replace(/'/g, "%27");

  // absolute urls
  if (/^(https?:)?\/\/[\w\.\-]+/i.test(href)) { return href; }
  // relative urls
  if (/^\/[\w\.\-]+/i.test(href)) { return href; }
  // anchors
  if (/^#[\w\.\-]+/i.test(href)) { return href; }
  // mailtos
  if (/^mailto:[\w\.\-@]+/i.test(href)) { return href; }
}

export function sanitize(text, whiteLister) {
  if (!text) return "";

  // Allow things like <3 and <_<
  text = text.replace(/<([^A-Za-z\/\!]|$)/g, "&lt;$1");

  const whiteList = whiteLister.getWhiteList();

  let hadIframe = false;
  let result = xss(text, {
    whiteList: whiteList.tagList,
    stripIgnoreTag: true,
    stripIgnoreTagBody: ['script', 'table'],
    onIgnoreTagAttr(tag, name, value) {
      hadIframe = hadIframe || tag === 'iframe';
      const forTag = whiteList.attrList[tag];
      if (forTag) {
        const forAttr = forTag[name];
        if ((forAttr && (forAttr.indexOf('*') !== -1 || forAttr.indexOf(value) !== -1)) ||
            (name.indexOf('data-') === 0 && forTag['data-*']) ||
            ((tag === 'a' && name === 'href') && hrefAllowed(value)) ||
            (tag === 'img' && name === 'src' && (/^data:image.*$/i.test(value) || hrefAllowed(value))) ||
            (tag === 'iframe' && name === 'src' && _validIframes.some(i => i.test(value)))) {
          return attr(name, value);
        }

        if (tag === 'iframe' && name === 'src') {
          return "-STRIP-";
        }
      }
    },
  });

  return result.replace(/\[removed\]/g, '')
               .replace(/\<iframe[^>]+\-STRIP\-[^>]*>[^<]*<\/iframe>/g, '')
               .replace(/&(?![#\w]+;)/g, '&amp;')
               .replace(/&#39;/g, "'")
               .replace(/ \/>/g, '>');
};

export function whiteListIframe(regexp) {
  _validIframes.push(regexp);
}

whiteListIframe(/^(https?:)?\/\/www\.google\.com\/maps\/embed\?.+/i);
whiteListIframe(/^(https?:)?\/\/www\.openstreetmap\.org\/export\/embed.html\?.+/i);
