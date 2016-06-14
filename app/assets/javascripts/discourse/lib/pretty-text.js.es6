import { cook } from 'discourse/lib/discourse-markdown';
import { sanitize } from 'discourse/lib/sanitizer';

const identity = value => value;

export default class {
  constructor(opts) {
    this.opts = opts || {};
  }

  cook(raw) {
    const { opts } = this;
    if (!raw || raw.length === 0) { return ""; }

    const sanitizer = (!!opts.sanitize) ? (opts.sanitizer || sanitize) : identity;
    const cookArgs = { traditionalMarkdownLinebreaks: opts.traditionalMarkdownLinebreaks,
                       defaultCodeLang: opts.defaultCodeLang || Discourse.SiteSettings.default_code_lang,
                       topicId: opts.topicId,
                       lookupAvatar: opts.lookupAvatar,
                       mentionLookup: opts.mentionLookup,
                       categoryHashtagLookup: opts.categoryHashtagLookup,
                       sanitizer };

    const result = cook(raw, cookArgs);
    return result ? result : "";
  }
};
