import { cook, setup } from 'pretty-text/engines/discourse-markdown';
import { sanitize } from 'pretty-text/sanitizer';
import WhiteLister from 'pretty-text/white-lister';

const identity = value => value;

export function buildOptions(state) {

  const { siteSettings, getURL, lookupAvatar, getTopicInfo, topicId, categoryHashtagLookup } = state;

  const features = {
    table: !!siteSettings.allow_html_tables,
    emoji: !!siteSettings.enable_emoji,
    'bold-italics': true,
    'auto-link': true,
    'mentions': true,
    'code': true,
    'bbcode': true,
    'quote': true,
    'html': true,
    'category-hashtag': true,
    'onebox': true,
    'censored': true,
    'newline': true
  };

  const acceptableCodeClasses = (siteSettings.highlighted_languages || "").split("|").concat(['auto', 'nohighlight']);
  return {
    sanitize: true,
    defaultCodeLang: siteSettings.default_code_lang,
    acceptableCodeClasses,
    emojiSet: siteSettings.emoji_set || "",
    getURL,
    features,
    lookupAvatar,
    getTopicInfo,
    topicId,
    categoryHashtagLookup,
    mentionLookup: state.mentionLookup
  };
}

export default class {
  constructor(opts) {
    opts = opts || {};
    this.opts = opts;
    this.features = opts.features || {};
    this.sanitizer = (!!opts.sanitize) ? (opts.sanitizer || sanitize) : identity;
    setup();
  }

  cook(raw) {
    const { opts } = this;
    if (!raw || raw.length === 0) { return ""; }

    const cookArgs = { traditionalMarkdownLinebreaks: opts.traditionalMarkdownLinebreaks,
                       defaultCodeLang: opts.defaultCodeLang,
                       topicId: opts.topicId,
                       lookupAvatar: opts.lookupAvatar,
                       mentionLookup: opts.mentionLookup,
                       categoryHashtagLookup: opts.categoryHashtagLookup,
                       features: this.features,
                       acceptableCodeClasses: opts.acceptableCodeClasses,
                       lookupAvatar: opts.lookupAvatar,
                       getURL: opts.getURL,
                       getTopicInfo: opts.getTopicInfo,
                       topicId: opts.topicId,
                       categoryHashtagLookup: opts.categoryHashtagLookup,
                       emojiSet: opts.emojiSet,
                       mentionLookup: opts.mentionLookup,
                       sanitizer: this.sanitizer };

    const result = cook(raw, cookArgs);
    return result ? result : "";
  }

  sanitize(html) {
    return this.sanitizer(html, new WhiteLister(this.features));
  }
};
